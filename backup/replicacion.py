"""
Modulo encargado de la replicacion de datos entre el nodo primario y los backups.
Maneja tanto la emision sincronica desde el primario como la recepcion en los backups.
"""

import threading

import Pyro5.api
import Pyro5.errors
from comun import config

TIMEOUT_REPLICACION_SEG = 0.8


class GestorReplicacion:
    def __init__(self, host: str, puerto: int, timeout: float = TIMEOUT_REPLICACION_SEG):
        self._host = host
        self._puerto = puerto
        self._timeout = timeout
        self._backups: list[Pyro5.api.Proxy] = []

    def conectar_backups(self):
        """Crea proxies hacia los demas nodos del cluster para replicarles estado."""
        self._backups = []
        for host, puerto in config.otros_nodos(self._host, self._puerto):
            proxy = Pyro5.api.Proxy(config.uri_de(host, puerto))
            proxy._pyroTimeout = self._timeout
            self._backups.append(proxy)

    def _replicar_un_backup(self, proxy: Pyro5.api.Proxy, estado: dict):
        """Replica el estado a un backup en segundo plano sin bloquear el flujo principal."""
        try:
            proxy._pyroClaimOwnership()
            proxy.replicar_estado(estado)
            print(
                f"[{self._puerto}][PRIMARIO][replicacion] Backup {proxy._pyroUri} replicado correctamente "
                f"(seq_op={estado['seq_op']})"
            )
        except (Pyro5.errors.CommunicationError, Pyro5.errors.PyroError):
            pass

    def replicar_a_backups(self, estado: dict):
        """
        Replica el estado a todos los backups de forma concurrente para no bloquear
        el flujo principal de la subasta, aun cuando uno o todos los backups esten caidos.
        """
        if not self._backups:
            return

        for proxy in self._backups:
            hilo = threading.Thread(
                target=self._replicar_un_backup,
                args=(proxy, estado),
                daemon=True,
            )
            hilo.start()

    def aplicar_estado(self, subasta, estado: dict):
        """
        El backup recibe el estado del primario y actualiza su subasta local.
        Verifica saltos de seq_op para detectar inconsistencias o mensajes perdidos.
        """
        if estado["seq_op"] not in (subasta.seq_op, subasta.seq_op + 1):
            print(
                f"[{self._puerto}][BACKUP][replicacion] ALERTA: salto de seq_op "
                f"({subasta.seq_op} -> {estado['seq_op']}), se perdio una actualizacion"
            )

        subasta.aplicar_estado_replicado(estado)
        print(
            f"[{self._puerto}][BACKUP][replicacion] Estado replicado: "
            f"mejor_oferta={subasta.mejor_oferta} mejor_postor={subasta.mejor_postor!r} seq_op={subasta.seq_op}"
        )
