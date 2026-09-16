"""
Modulo encargado de la deteccion de caidas (heartbeat) y del algoritmo
de eleccion de un nuevo lider/primario entre los nodos vivos.
"""

import threading
import time
from typing import Callable

import Pyro5.api
import Pyro5.errors
from comun import config

INTERVALO_HEARTBEAT_SEG = 0.5
TIMEOUT_PING_SEG = 0.8


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
                self.ejecutar_eleccion()

    def prioridad_nodo(self, nodo: dict) -> tuple[int, int, int]:
        host, puerto = nodo["host"], nodo["puerto"]
        idx = config.NODOS.index((host, puerto)) if (host, puerto) in config.NODOS else 0
        return (
            int(nodo.get("seq_op", 0)),          # Prioridad principal: replica mas actualizada en subasta
            int(nodo.get("clock_lamport", 0)),   # Reloj de Lamport
            idx,                                 # Desempate determinista por indice
        )

    def ejecutar_eleccion(self):
        """
        Consulta todos los nodos de config.NODOS para ver cuales estan vivos.
        Elige como nuevo primario al nodo cuya ultima replica tiene seq_op y reloj de
        Lamport mas alto; en caso de empate, usa la prioridad del cluster.
        """
        with self._lock:
            if self._en_eleccion:
                return
            self._en_eleccion = True

        try:
            vivos = []
            lock_vivos = threading.Lock()
            threads = []

            # Estado local del propio nodo (sin necesidad de RPC a sí mismo)
            clock_propio = 0
            seq_propio = 0
            if self._obtener_estado_local:
                clock_propio, seq_propio = self._obtener_estado_local()

            vivos.append(
                {
                    "host": self._host,
                    "puerto": self._puerto,
                    "clock_lamport": clock_propio,
                    "seq_op": seq_propio,
                }
            )

            def consultar_nodo(h: str, p: int):
                try:
                    proxy = Pyro5.api.Proxy(config.uri_de(h, p))
                    proxy._pyroTimeout = self._timeout
                    proxy.ping()
                    estado = proxy.obtener_estado()
                    with lock_vivos:
                        vivos.append(
                            {
                                "host": h,
                                "puerto": p,
                                "clock_lamport": int(estado.get("clock_lamport", 0)),
                                "seq_op": int(estado.get("seq_op", 0)),
                            }
                        )
                except Exception:
                    pass

            for host, puerto in config.NODOS:
                if (host, puerto) == (self._host, self._puerto):
                    continue
                t = threading.Thread(target=consultar_nodo, args=(host, puerto), daemon=True)
                threads.append(t)
                t.start()

            for t in threads:
                t.join(timeout=self._timeout + 0.2)

            if not vivos:
                return

            elegido = max(vivos, key=lambda nodo: self.prioridad_nodo(nodo))
            nodo_elegido = (elegido["host"], elegido["puerto"])

            con_promocion = False
            with self._lock:
                self._primario = nodo_elegido
                if nodo_elegido == (self._host, self._puerto):
                    if not self._es_primario:
                        self._es_primario = True
                        con_promocion = True
                        print(
                            f"[{self._puerto}][PRIMARIO][eleccion] Eleccion completada: "
                            f"¡Este nodo ({self._host}:{self._puerto}) fue electo nuevo primario! "
                            f"(seq_op={elegido['seq_op']}, clock_lamport={elegido['clock_lamport']})"
                        )
                else:
                    print(
                        f"[{self._puerto}][BACKUP][eleccion] Eleccion completada: "
                        f"nuevo primario en {nodo_elegido[0]}:{nodo_elegido[1]} "
                        f"(seq_op={elegido['seq_op']}, clock_lamport={elegido['clock_lamport']})"
                    )

            if con_promocion and self._on_promocion:
                self._on_promocion()
        finally:
            with self._lock:
                self._en_eleccion = False

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
            self.ejecutar_eleccion()
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
