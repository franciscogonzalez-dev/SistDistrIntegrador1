"""
Modulo encargado de la deteccion de caidas (heartbeat) y del algoritmo
de eleccion de un nuevo lider/primario entre los nodos vivos.

Implementa una variante del algoritmo Bully donde el criterio de "mayor ID"
es reemplazado por la tupla (seq_op, clock_lamport, idx_en_cluster), priorizando
la replica con el estado mas actualizado de la subasta.

Flujo de mensajes (equivalente al Bully clasico):
  1. ELECTION : el nodo que detecta la caida del primario consulta su prioridad
                y envia iniciar_election() a todos los nodos con MAYOR prioridad.
  2. OK       : un nodo que recibe ELECTION y tiene mayor prioridad responde OK
                y lanza su propia eleccion. El iniciador original se retira.
  3. COORDINATOR: el nodo que no recibio ningun OK (es el de mayor prioridad
                entre los vivos) se proclama coordinador y notifica a todos.
"""

import threading
import time
from typing import Callable

import Pyro5.api
import Pyro5.errors
from comun import config

INTERVALO_HEARTBEAT_SEG = 0.5
TIMEOUT_PING_SEG = 0.8
# Tiempo que espera un nodo para recibir OK antes de proclamarse coordinador.
TIMEOUT_OK_SEG = 2.0


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

        # Evento que se activa cuando se recibe un mensaje OK de un nodo superior.
        # Mientras este seteado, el nodo sabe que hay alguien de mayor prioridad
        # haciendose cargo de la eleccion y no debe proclamarse coordinador.
        self._recibio_ok = threading.Event()

    # ------------------------------------------------------------------
    # Vigilancia del primario (heartbeat)
    # ------------------------------------------------------------------

    def iniciar_vigilancia(self):
        """Inicia el hilo de monitoreo continuo si el nodo arranca como backup."""
        if not self.es_primario():
            self._hilo_vigilancia = threading.Thread(target=self._vigila_primario, daemon=True)
            self._hilo_vigilancia.start()

    def _vigila_primario(self):
        """Heartbeat periodico al primario mientras este nodo sea backup."""
        while not self.es_primario():
            time.sleep(self._intervalo)
            with self._lock:
                if self._es_primario:
                    break
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
    # Calculo de prioridad
    # ------------------------------------------------------------------

    def prioridad_nodo(self, nodo: dict) -> tuple[int, int, int]:
        host, puerto = nodo["host"], nodo["puerto"]
        idx = config.NODOS.index((host, puerto)) if (host, puerto) in config.NODOS else 0
        return (
            int(nodo.get("seq_op", 0)),          # Prioridad principal: replica mas actualizada en subasta
            int(nodo.get("clock_lamport", 0)),   # Reloj de Lamport
            idx,                                 # Desempate determinista por indice
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

    def _prioridad_propia(self) -> tuple[int, int, int]:
        return self.prioridad_nodo(self._estado_local_como_dict())

    # ------------------------------------------------------------------
    # Fase 1 — ELECTION: avisar a los nodos de mayor prioridad
    # ------------------------------------------------------------------

    def iniciar_election(self):
        """
        Punto de entrada cuando este nodo detecta que el primario no responde.

        Envia el mensaje ELECTION a todos los nodos con mayor prioridad que el
        propio. Si alguno responde OK, este nodo se retira y espera que el
        superior se proclame coordinador. Si nadie responde, este nodo se
        proclama coordinador.
        """
        with self._lock:
            if self._en_eleccion:
                return
            self._en_eleccion = True

        self._recibio_ok.clear()

        try:
            mi_prioridad = self._prioridad_propia()
            nodos_superiores = []

            for host, puerto in config.NODOS:
                if (host, puerto) == (self._host, self._puerto):
                    continue
                # Consultar el estado del nodo para comparar prioridad
                try:
                    proxy = Pyro5.api.Proxy(config.uri_de(host, puerto))
                    proxy._pyroTimeout = self._timeout
                    proxy.ping()
                    estado = proxy.obtener_estado()
                    nodo_dict = {
                        "host": host,
                        "puerto": puerto,
                        "clock_lamport": int(estado.get("clock_lamport", 0)),
                        "seq_op": int(estado.get("seq_op", 0)),
                    }
                    if self.prioridad_nodo(nodo_dict) > mi_prioridad:
                        nodos_superiores.append((host, puerto, proxy))
                except Exception:
                    # Nodo caido o inaccesible: se ignora
                    pass

            if not nodos_superiores:
                # No hay nadie con mayor prioridad vivo: me proclamo coordinador
                print(
                    f"[{self._puerto}][eleccion] Ningun nodo con mayor prioridad encontrado. "
                    "Enviando COORDINATOR..."
                )
                self._proclamar_coordinador()
                return

            # Enviar ELECTION a todos los nodos de mayor prioridad
            print(
                f"[{self._puerto}][eleccion] Enviando ELECTION a "
                f"{[(h, p) for h, p, _ in nodos_superiores]}"
            )

            def enviar_election(h, p, proxy):
                try:
                    proxy._pyroClaimOwnership()
                    proxy.recibir_election(self._host, self._puerto)
                except Exception:
                    pass

            hilos = []
            for h, p, proxy in nodos_superiores:
                t = threading.Thread(target=enviar_election, args=(h, p, proxy), daemon=True)
                hilos.append(t)
                t.start()

            for t in hilos:
                t.join(timeout=self._timeout + 0.2)

            # Esperar OK durante TIMEOUT_OK_SEG
            recibio = self._recibio_ok.wait(timeout=TIMEOUT_OK_SEG)

            if recibio:
                # Un nodo superior esta a cargo: este nodo espera el COORDINATOR
                print(
                    f"[{self._puerto}][eleccion] Recibi OK de un nodo superior. "
                    "Esperando mensaje COORDINATOR..."
                )
                # No hacer nada: el coordinador llegara via recibir_coordinator()
            else:
                # Nadie respondio OK a tiempo: me proclamo coordinador
                print(
                    f"[{self._puerto}][eleccion] Timeout esperando OK. "
                    "Enviando COORDINATOR..."
                )
                self._proclamar_coordinador()

        finally:
            with self._lock:
                self._en_eleccion = False

    # ------------------------------------------------------------------
    # Fase 2 — OK: responder a un mensaje ELECTION recibido
    # ------------------------------------------------------------------

    def recibir_election(self, host_emisor: str, puerto_emisor: int):
        """
        Llamado por Pyro5 cuando otro nodo nos envia un mensaje ELECTION.

        Si este nodo tiene mayor prioridad que el emisor, responde OK y lanza
        su propia eleccion (si aun no esta en una).
        """
        try:
            # Responder OK al emisor de inmediato
            proxy = Pyro5.api.Proxy(config.uri_de(host_emisor, puerto_emisor))
            proxy._pyroTimeout = self._timeout
            proxy.recibir_ok(self._host, self._puerto)
            print(
                f"[{self._puerto}][eleccion] Recibi ELECTION de {host_emisor}:{puerto_emisor}. "
                "Respondo OK y lanzo mi propia eleccion."
            )
        except Exception:
            pass

        # Iniciar eleccion propia en segundo plano para no bloquear al emisor
        threading.Thread(target=self.iniciar_election, daemon=True).start()

    # ------------------------------------------------------------------
    # Recepcion del mensaje OK (señal de que hay alguien superior)
    # ------------------------------------------------------------------

    def recibir_ok(self, host_emisor: str, puerto_emisor: int):
        """
        Llamado por Pyro5 cuando un nodo superior nos responde OK a nuestro
        mensaje ELECTION. Indica que hay alguien de mayor prioridad haciendose
        cargo: este nodo debe dejar de intentar proclamarse coordinador.
        """
        print(
            f"[{self._puerto}][eleccion] Recibi OK de {host_emisor}:{puerto_emisor}. "
            "Me retiro de la eleccion."
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
        with self._lock:
            self._primario = (self._host, self._puerto)
            if not self._es_primario:
                self._es_primario = True
                con_promocion = True
                clock_propio, seq_propio = (0, 0)
                if self._obtener_estado_local:
                    clock_propio, seq_propio = self._obtener_estado_local()
                print(
                    f"[{self._puerto}][PRIMARIO][eleccion] ¡Este nodo fue electo nuevo primario! "
                    f"(seq_op={seq_propio}, clock_lamport={clock_propio})"
                )

        # Notificar a todos los demas nodos (mensaje COORDINATOR)
        def notificar_coordinator(h: str, p: int):
            try:
                proxy = Pyro5.api.Proxy(config.uri_de(h, p))
                proxy._pyroTimeout = self._timeout
                proxy.recibir_coordinator(self._host, self._puerto)
            except Exception:
                pass

        for host, puerto in config.NODOS:
            if (host, puerto) == (self._host, self._puerto):
                continue
            threading.Thread(
                target=notificar_coordinator, args=(host, puerto), daemon=True
            ).start()

        if con_promocion and self._on_promocion:
            self._on_promocion()

    def recibir_coordinator(self, host_coordinador: str, puerto_coordinador: int):
        """
        Llamado por Pyro5 cuando el nuevo coordinador anuncia su eleccion.
        Actualiza el primario conocido y cancela cualquier eleccion en curso.
        """
        print(
            f"[{self._puerto}][BACKUP][eleccion] Recibi COORDINATOR: "
            f"nuevo primario en {host_coordinador}:{puerto_coordinador}"
        )
        with self._lock:
            self._primario = (host_coordinador, puerto_coordinador)
            if (host_coordinador, puerto_coordinador) != (self._host, self._puerto):
                self._es_primario = False
            self._en_eleccion = False
        # Cancelar espera de OK si estabamos aguardando (el coordinador ya fue elegido)
        self._recibio_ok.set()

    # ------------------------------------------------------------------
    # Metodos de consulta y actualizacion del primario
    # ------------------------------------------------------------------

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
            # Si el primario no respondio, disparar eleccion de inmediato
            self.iniciar_election()
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
