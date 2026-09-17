"""
Modulo encargado de la deteccion de caidas (heartbeat) y del algoritmo
de eleccion de un nuevo lider/primario entre los nodos vivos.

Implementa una variante del algoritmo Bully adaptada a replicacion por estado:
en lugar de basarse exclusivamente en un ID estatico, el criterio de prioridad
es la tupla (seq_op, clock_lamport, idx_cluster, id_nodo), priorizando la replica
con el estado mas actualizado de la subasta y garantizando un orden total estricto
para evitar condiciones de empate o Split-Brain.

Flujo de mensajes (Bully modificado por estado):
  1. ELECTION  : El nodo que detecta la caida del primario consulta el estado de los
                 nodos vivos mediante una llamada ligera (obtener_info_eleccion).
                 Si este nodo tiene la mayor prioridad de todos, se proclama COORDINATOR.
                 Si existen nodos con MAYOR prioridad, les envia ELECTION.
  2. OK        : Un nodo que recibe ELECTION verifica si efectivamente su prioridad
                 es mayor que la del emisor. Si es mayor, responde OK y lanza su propia
                 eleccion. El emisor original sabe que un superior se hizo cargo y
                 espera el COORDINATOR con un timeout.
  3. COORDINATOR: El nodo de mayor prioridad (o que no recibio ningun OK de superiores)
                 se proclama coordinador y notifica a todos los nodos del cluster.
                 Si el superior falla y el COORDINATOR no llega a tiempo, se reinicia
                 la eleccion.
"""

import threading
import time
from typing import Callable

import Pyro5.api
import Pyro5.errors
from comun import config

INTERVALO_HEARTBEAT_SEG = 1.0
TIMEOUT_PING_SEG = 1.5
TIMEOUT_CONSULTA_ESTADO_SEG = 1.0
TIMEOUT_OK_SEG = 2.5
TIMEOUT_COORDINATOR_SEG = 4.0


class MonitorEleccion:
    def __init__(
        self,
        host: str,
        puerto: int,
        es_primario: bool,
        on_promocion: Callable[[], None] | None = None,
        obtener_estado_local: Callable[[], tuple[int, int]] | None = None,
        intervalo_heartbeat: float = INTERVALO_HEARTBEAT_SEG,
        timeout: float = TIMEOUT_PING_SEG,
    ):
        self._host = host
        self._puerto = puerto
        self._es_primario = es_primario
        self._primario = (host, puerto) if es_primario else config.NODOS[0]
        self._on_promocion = on_promocion
        self._obtener_estado_local = obtener_estado_local
        self._intervalo = intervalo_heartbeat
        self._timeout = timeout
        self._lock = threading.Lock()
        self._hilo_vigilancia: threading.Thread | None = None
        self._en_eleccion = False

        # Eventos para coordinar las fases del algoritmo Bully
        self._recibio_ok = threading.Event()
        self._evento_coordinador = threading.Event()

    # ------------------------------------------------------------------
    # Vigilancia del primario (heartbeat)
    # ------------------------------------------------------------------

    def iniciar_vigilancia(self):
        """Inicia el hilo de monitoreo continuo si este nodo opera como backup."""
        with self._lock:
            if self._es_primario:
                return
            if self._hilo_vigilancia and self._hilo_vigilancia.is_alive():
                return
            self._hilo_vigilancia = threading.Thread(target=self._vigila_primario, daemon=True)
            self._hilo_vigilancia.start()

    def _vigila_primario(self):
        """Heartbeat periodico al primario mientras este nodo sea backup."""
        while not self.es_primario():
            time.sleep(self._intervalo)
            if self.es_primario():
                break

            with self._lock:
                host_p, puerto_p = self._primario

            # Si el primario guardado soy yo mismo, no hay nada que pinguear
            if (host_p, puerto_p) == (self._host, self._puerto):
                continue

            proxy = Pyro5.api.Proxy(config.uri_de(host_p, puerto_p))
            proxy._pyroTimeout = self._timeout
            try:
                proxy.ping()
            except (Pyro5.errors.CommunicationError, Pyro5.errors.PyroError):
                print(
                    f"[{self._puerto}][BACKUP][eleccion] Primario en {host_p}:{puerto_p} no responde. "
                    "Iniciando eleccion..."
                )
                self.iniciar_election()

    # ------------------------------------------------------------------
    # Calculo de prioridad con orden total estricto
    # ------------------------------------------------------------------

    def prioridad_nodo(self, nodo: dict) -> tuple[int, int, int, str]:
        """
        Orden total determinista:
          1. seq_op: estado de operaciones de subasta mas avanzado.
          2. clock_lamport: reloj logico mas avanzado ante igual seq_op.
          3. idx: posicion en lista de cluster (si coincide).
          4. id unico 'host:puerto': garantiza que NUNCA exista empate.
        """
        host = str(nodo.get("host", ""))
        puerto = int(nodo.get("puerto", 0))

        idx = -1
        for i, (h, p) in enumerate(config.NODOS):
            if (h == host and p == puerto) or (p == puerto):
                idx = i
                break

        return (
            int(nodo.get("seq_op", 0)),
            int(nodo.get("clock_lamport", 0)),
            idx,
            f"{host}:{puerto}",
        )

    def _estado_local_como_dict(self) -> dict:
        """Construye el dict de estado de este nodo para comparaciones de prioridad."""
        clock_propio, seq_propio = (0, 0)
        if self._obtener_estado_local:
            clock_propio, seq_propio = self._obtener_estado_local()
        return {
            "host": self._host,
            "puerto": self._puerto,
            "clock_lamport": clock_propio,
            "seq_op": seq_propio,
        }

    def _prioridad_propia(self) -> tuple[int, int, int, str]:
        return self.prioridad_nodo(self._estado_local_como_dict())

    # ------------------------------------------------------------------
    # Fase 1 — ELECTION: avisar a los nodos de mayor prioridad
    # ------------------------------------------------------------------

    def iniciar_election(self):
        """
        Punto de entrada cuando este nodo detecta que el primario no responde
        o al ser notificado por otro nodo.
        """
        with self._lock:
            if self._en_eleccion:
                return
            self._en_eleccion = True

        self._recibio_ok.clear()
        self._evento_coordinador.clear()

        try:
            mi_info = self._estado_local_como_dict()
            mi_prioridad = self.prioridad_nodo(mi_info)

            # Consultar concurrentemente el estado ligero de los demas nodos
            nodos_superiores = []
            cand_respuestas = {}

            def consultar_nodo(h, p):
                try:
                    proxy = Pyro5.api.Proxy(config.uri_de(h, p))
                    proxy._pyroTimeout = TIMEOUT_CONSULTA_ESTADO_SEG
                    # Usar endpoint ligero que no hace pings ni bloquea
                    if hasattr(proxy, "obtener_info_eleccion"):
                        info = proxy.obtener_info_eleccion()
                    else:
                        info = proxy.obtener_estado()

                    info_dict = {
                        "host": h,
                        "puerto": p,
                        "clock_lamport": int(info.get("clock_lamport", 0)),
                        "seq_op": int(info.get("seq_op", 0)),
                    }
                    cand_respuestas[(h, p)] = (info_dict, proxy)
                except Exception:
                    pass

            hilos_consulta = []
            for host, puerto in config.otros_nodos(self._host, self._puerto):
                t = threading.Thread(target=consultar_nodo, args=(host, puerto), daemon=True)
                hilos_consulta.append(t)
                t.start()

            for t in hilos_consulta:
                t.join(timeout=TIMEOUT_CONSULTA_ESTADO_SEG + 0.2)

            for (h, p), (info_dict, proxy) in cand_respuestas.items():
                if self.prioridad_nodo(info_dict) > mi_prioridad:
                    nodos_superiores.append((h, p, proxy))

            if not nodos_superiores:
                # No hay ningun nodo vivo con mayor prioridad: este nodo se proclama coordinador
                print(
                    f"[{self._puerto}][eleccion] Ningun nodo vivo con mayor prioridad. "
                    "Proclamandose COORDINATOR..."
                )
                self._proclamar_coordinador()
                return

            # Enviar ELECTION a los nodos con mayor prioridad
            print(
                f"[{self._puerto}][eleccion] Enviando ELECTION a nodos superiores: "
                f"{[(h, p) for h, p, _ in nodos_superiores]}"
            )

            def enviar_election(h, p, proxy):
                try:
                    proxy._pyroClaimOwnership()
                    proxy._pyroTimeout = self._timeout
                    # Enviar ELECTION incluyendo la informacion de este nodo emisor
                    ok = proxy.recibir_election(self._host, self._puerto, mi_info)
                    if ok:
                        self._recibio_ok.set()
                except Exception:
                    pass

            hilos_election = []
            for h, p, proxy in nodos_superiores:
                t = threading.Thread(target=enviar_election, args=(h, p, proxy), daemon=True)
                hilos_election.append(t)
                t.start()

            for t in hilos_election:
                t.join(timeout=self._timeout + 0.2)

            # Esperar confirmacion OK durante TIMEOUT_OK_SEG
            recibio = self._recibio_ok.wait(timeout=TIMEOUT_OK_SEG)

            if not recibio:
                # Ningun nodo superior respondio OK a tiempo (cayeron o no responden)
                print(
                    f"[{self._puerto}][eleccion] Timeout esperando OK de nodos superiores. "
                    "Proclamandose COORDINATOR..."
                )
                self._proclamar_coordinador()
                return

            # Si recibio OK, un superior se hace cargo de la eleccion: esperar mensaje COORDINATOR
            print(
                f"[{self._puerto}][eleccion] Recibi OK de un nodo superior. "
                "Esperando mensaje COORDINATOR..."
            )
            recibio_coord = self._evento_coordinador.wait(timeout=TIMEOUT_COORDINATOR_SEG)
            if not recibio_coord:
                # El nodo superior respondio OK pero no llego a enviar COORDINATOR a tiempo
                print(
                    f"[{self._puerto}][eleccion] Timeout esperando COORDINATOR del superior. "
                    "Reiniciando eleccion..."
                )
                threading.Thread(target=self.iniciar_election, daemon=True).start()

        finally:
            with self._lock:
                self._en_eleccion = False

    # ------------------------------------------------------------------
    # Fase 2 — OK: responder a un mensaje ELECTION recibido
    # ------------------------------------------------------------------

    def recibir_election(
        self, host_emisor: str, puerto_emisor: int, info_emisor: dict | None = None
    ) -> bool:
        """
        Llamado por Pyro5 cuando otro nodo nos envia un mensaje ELECTION.

        Si este nodo tiene mayor prioridad que el emisor:
          - Responde OK (True)
          - Lanza su propia eleccion para continuar el algoritmo Bully.
        Si este nodo NO tiene mayor prioridad:
          - Rechaza el mensaje (False).
        """
        mi_info = self._estado_local_como_dict()
        mi_prioridad = self.prioridad_nodo(mi_info)

        if info_emisor is not None:
            prioridad_emisor = self.prioridad_nodo(info_emisor)
            if mi_prioridad <= prioridad_emisor:
                # No tenemos mayor prioridad que el emisor: no corresponde responder OK
                return False

        print(
            f"[{self._puerto}][eleccion] Recibi ELECTION de {host_emisor}:{puerto_emisor}. "
            "Tengo mayor prioridad: respondo OK y continuo la eleccion."
        )

        # Enviar OK explícito al emisor por compatibilidad RPC
        try:
            proxy = Pyro5.api.Proxy(config.uri_de(host_emisor, puerto_emisor))
            proxy._pyroTimeout = self._timeout
            proxy.recibir_ok(self._host, self._puerto)
        except Exception:
            pass

        # Iniciar eleccion propia en segundo plano
        threading.Thread(target=self.iniciar_election, daemon=True).start()
        return True

    # ------------------------------------------------------------------
    # Recepcion del mensaje OK
    # ------------------------------------------------------------------

    def recibir_ok(self, host_emisor: str, puerto_emisor: int):
        """
        Llamado por Pyro5 cuando un nodo superior nos responde OK a nuestro
        mensaje ELECTION.
        """
        print(
            f"[{self._puerto}][eleccion] Recibi OK de {host_emisor}:{puerto_emisor}. "
            "Nodo superior activo en la eleccion."
        )
        self._recibio_ok.set()

    # ------------------------------------------------------------------
    # Fase 3 — COORDINATOR: proclamarse y notificar a todos
    # ------------------------------------------------------------------

    def _proclamar_coordinador(self):
        """
        Este nodo es el de mayor prioridad entre los vivos. Se proclama primario
        y envia el mensaje COORDINATOR a todos los demas nodos del cluster.
        """
        con_promocion = False
        mi_info = self._estado_local_como_dict()

        with self._lock:
            self._primario = (self._host, self._puerto)
            if not self._es_primario:
                self._es_primario = True
                con_promocion = True
                clock_propio, seq_propio = mi_info["clock_lamport"], mi_info["seq_op"]
                print(
                    f"[{self._puerto}][PRIMARIO][eleccion] ¡Este nodo fue electo nuevo primario! "
                    f"(seq_op={seq_propio}, clock_lamport={clock_propio})"
                )
            self._evento_coordinador.set()

        # Notificar a todos los demas nodos (mensaje COORDINATOR)
        def notificar_coordinator(h: str, p: int):
            try:
                proxy = Pyro5.api.Proxy(config.uri_de(h, p))
                proxy._pyroTimeout = self._timeout
                proxy.recibir_coordinator(self._host, self._puerto, mi_info)
            except Exception:
                pass

        for host, puerto in config.otros_nodos(self._host, self._puerto):
            threading.Thread(
                target=notificar_coordinator, args=(host, puerto), daemon=True
            ).start()

        if con_promocion and self._on_promocion:
            self._on_promocion()

    def recibir_coordinator(
        self, host_coordinador: str, puerto_coordinador: int, info_coordinador: dict | None = None
    ):
        """
        Llamado por Pyro5 cuando un nodo anuncia que fue electo coordinador.
        """
        if (host_coordinador, puerto_coordinador) == (self._host, self._puerto):
            return

        # Si ya somos primario y el coordinador anunciado tiene menor prioridad que nosotros,
        # rechazamos para evitar que un nodo desactualizado degrade al lider valido
        with self._lock:
            es_prim = self._es_primario

        if es_prim and info_coordinador is not None:
            if self.prioridad_nodo(info_coordinador) < self._prioridad_propia():
                print(
                    f"[{self._puerto}][PRIMARIO][eleccion] COORDINATOR rechazado de "
                    f"{host_coordinador}:{puerto_coordinador} (prioridad menor a la local)."
                )
                return

        print(
            f"[{self._puerto}][BACKUP][eleccion] Recibi COORDINATOR: "
            f"nuevo primario en {host_coordinador}:{puerto_coordinador}"
        )

        with self._lock:
            self._primario = (host_coordinador, puerto_coordinador)
            self._es_primario = False
            self._en_eleccion = False

        self._evento_coordinador.set()
        self._recibio_ok.set()

        # Como ahora este nodo es backup, debe volver a vigilar al nuevo primario
        self.iniciar_vigilancia()

    # ------------------------------------------------------------------
    # Metodos de consulta y actualizacion del primario
    # ------------------------------------------------------------------

    def primario_actual(self) -> tuple[str, int]:
        """Devuelve de inmediato la tupla (host, puerto) del primario conocido sin bloquear."""
        with self._lock:
            return self._primario

    def ubicacion_primario(self) -> dict:
        """Devuelve el host y puerto del primario actual, verificando que responda."""
        with self._lock:
            es_prim = self._es_primario
            host, puerto = self._primario

        if es_prim or (host, puerto) == (self._host, self._puerto):
            return {"host": self._host, "puerto": self._puerto}

        # Verificar si el primario guardado sigue respondiendo
        proxy = Pyro5.api.Proxy(config.uri_de(host, puerto))
        proxy._pyroTimeout = self._timeout
        try:
            proxy.ping()
            return {"host": host, "puerto": puerto}
        except Exception:
            # Si el primario no respondio, disparar eleccion en segundo plano
            threading.Thread(target=self.iniciar_election, daemon=True).start()
            with self._lock:
                h, p = self._primario
                return {"host": h, "puerto": p}

    def actualizar_primario(self, host: str, puerto: int):
        """Actualiza la direccion del primario conocido (usado al recibir replicacion)."""
        with self._lock:
            self._primario = (host, puerto)

    def es_primario(self) -> bool:
        with self._lock:
            return self._es_primario
