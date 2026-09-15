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

INTERVALO_HEARTBEAT_SEG = 1.0
TIMEOUT_PING_SEG = 1.5


class MonitorEleccion:
    def __init__(
        self,
        host: str,
        puerto: int,
        es_primario: bool,
        on_promocion: Callable[[], None] | None = None,
        intervalo_heartbeat: float = INTERVALO_HEARTBEAT_SEG,
        timeout: float = TIMEOUT_PING_SEG,
    ):
        self._host = host
        self._puerto = puerto
        self._es_primario = es_primario
        self._primario = (host, puerto) if es_primario else config.NODOS[0]
        self._on_promocion = on_promocion
        self._intervalo = intervalo_heartbeat
        self._timeout = timeout
        self._lock = threading.Lock()
        self._hilo_vigilancia: threading.Thread | None = None

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
                host_p, puerto_p = self._primario

            proxy = Pyro5.api.Proxy(config.uri_de(host_p, puerto_p))
            proxy._pyroTimeout = self._timeout
            try:
                proxy.ping()
            except Pyro5.errors.CommunicationError:
                print(
                    f"[{self._puerto}][BACKUP][eleccion] Primario en {host_p}:{puerto_p} no responde. "
                    "Iniciando eleccion..."
                )
                self.ejecutar_eleccion()

    def ejecutar_eleccion(self):
        """
        Consulta todos los nodos de config.NODOS para ver cuales estan vivos.
        Elige al nodo de mayor prioridad (mayor indice en config.NODOS).
        Si el elegido es este nodo, se auto-promueve a primario y llama a on_promocion.
        """
        vivos = []
        for host, puerto in config.NODOS:
            proxy = Pyro5.api.Proxy(config.uri_de(host, puerto))
            proxy._pyroTimeout = self._timeout
            try:
                proxy.ping()
                vivos.append((host, puerto))
            except Pyro5.errors.CommunicationError:
                continue

        if not vivos:
            return

        elegido = max(vivos, key=lambda nodo: config.NODOS.index(nodo))
        with self._lock:
            self._primario = elegido
            if elegido != (self._host, self._puerto) or self._es_primario:
                print(
                    f"[{self._puerto}][BACKUP][eleccion] Eleccion completada: "
                    f"nuevo primario en {elegido[0]}:{elegido[1]}"
                )
                return
            self._es_primario = True

        print(
            f"[{self._puerto}][PRIMARIO][eleccion] Eleccion completada: "
            f"¡Este nodo ({self._host}:{self._puerto}) fue electo nuevo primario!"
        )
        if self._on_promocion:
            self._on_promocion()

    def ubicacion_primario(self) -> dict:
        """Devuelve el host y puerto del primario actual."""
        with self._lock:
            host, puerto = self._primario
            return {"host": host, "puerto": puerto}

    def actualizar_primario(self, host: str, puerto: int):
        """Actualiza la direccion del primario conocido (usado al recibir replicacion)."""
        with self._lock:
            self._primario = (host, puerto)

    def es_primario(self) -> bool:
        with self._lock:
            return self._es_primario
