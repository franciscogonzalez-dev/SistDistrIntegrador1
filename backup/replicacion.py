"""
Modulo encargado de la replicacion de datos entre el nodo primario y los backups.
Maneja tanto la emision sincronica desde el primario como la recepcion en los backups.
"""

import Pyro5.api
import Pyro5.errors
from comun import config

TIMEOUT_REPLICACION_SEG = 1.5


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

    def replicar_a_backups(self, estado: dict):
        """
        El primario le envia el estado nuevo a cada backup.
        Si algun backup no contesta a tiempo, se continua sin trabar la subasta.
        """
        for proxy in self._backups:
            try:
                proxy._pyroClaimOwnership()
                proxy.replicar_estado(estado)
            except Pyro5.errors.CommunicationError:
                print(
                    f"[{self._puerto}][PRIMARIO][replicacion] Backup {proxy._pyroUri} no respondio, "
                    f"se sigue sin el (seq_op={estado['seq_op']})"
                )

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
