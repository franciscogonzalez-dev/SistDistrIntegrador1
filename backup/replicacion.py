"""
Modulo encargado de la replicacion de datos entre el nodo primario y los backups.

Replicacion SEMI-SINCRONICA: el primario despacha la actualizacion a todos los
backups en paralelo, pero solo bloquea al llamador hasta que llega el ACK del
PRIMERO que responda (o hasta agotar TIMEOUT_ESPERA_PRIMER_ACK_SEG si ninguno
llega a tiempo, para no colgar el sistema si todos los backups estan caidos).
El resto de los backups terminan de actualizarse en paralelo, sin que nadie
mas los espere.

Es un punto intermedio entre:
  - sincronica estricta (esperar a TODOS): un solo backup caido bloquea al
    cluster entero.
  - asincronica pura (no esperar a ninguno): latencia minima, pero si el
    primario muere en el instante entre aceptar la oferta y que los hilos de
    replicacion lleguen a algun backup, esa oferta se pierde en todas las
    replicas.
Con "esperar al primero" se gana la garantia de que, si el primario responde
"aceptada" al cliente, al menos una replica YA tiene ese estado guardado -
a cambio de un costo de latencia acotado al viaje de ida y vuelta hacia la
replica mas rapida (en vez de a todas).
"""

import threading

import Pyro5.api
import Pyro5.errors
from comun import config

TIMEOUT_REPLICACION_SEG = 0.8
# Tope maximo esperando el ACK de la primera replica. Si se cae/tarda todo el
# cluster de backups, no tiene sentido bloquear al cliente para siempre: se
# sigue de largo (degradando, para ese caso puntual, a un comportamiento
# equivalente al asincronico) para no perder alta disponibilidad.
TIMEOUT_ESPERA_PRIMER_ACK_SEG = TIMEOUT_REPLICACION_SEG + 0.3


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

    def _replicar_un_backup(self, proxy: Pyro5.api.Proxy, estado: dict) -> bool:
        """Replica el estado a un backup. Devuelve True si confirmo (ACK), False si fallo."""
        try:
            proxy._pyroClaimOwnership()
            proxy.replicar_estado(estado)
            print(
                f"[{self._puerto}][PRIMARIO][replicacion] Backup {proxy._pyroUri} replicado correctamente "
                f"(seq_op={estado['seq_op']})"
            )
            return True
        except (Pyro5.errors.CommunicationError, Pyro5.errors.PyroError):
            return False

    def replicar_a_backups(self, estado: dict) -> bool:
        """
        Replica el estado a todos los backups en paralelo, pero bloquea al
        llamador solo hasta el ACK del primero que confirme (o hasta el
        timeout de espera si ninguno llega a tiempo). Los backups restantes
        siguen actualizandose en segundo plano aunque esta funcion ya haya
        retornado.

        Devuelve True si al menos un backup confirmo dentro del timeout,
        False si ninguno lo hizo (cluster de backups caido o muy lento).
        """
        if not self._backups:
            return False

        primer_ack = threading.Event()

        def replicar_y_avisar(proxy: Pyro5.api.Proxy):
            if self._replicar_un_backup(proxy, estado):
                primer_ack.set()

        for proxy in self._backups:
            hilo = threading.Thread(
                target=replicar_y_avisar,
                args=(proxy,),
                daemon=True,
            )
            hilo.start()

        return primer_ack.wait(timeout=TIMEOUT_ESPERA_PRIMER_ACK_SEG)

    def aplicar_estado(self, subasta, estado: dict):
        """
        El backup recibe el estado del primario y actualiza su subasta local.
        Verifica monotonicidad y saltos de seq_op para detectar inconsistencias.
        """
        # Descartar estados obsoletos (regresión en el tiempo lógico / mensaje viejo)
        if estado["seq_op"] < subasta.seq_op:
            print(
                f"[{self._puerto}][BACKUP][replicacion] RECHAZADO: estado obsoleto ignorado "
                f"(local={subasta.seq_op}, recibido={estado['seq_op']})"
            )
            return

        if estado["seq_op"] > subasta.seq_op + 1:
            print(
                f"[{self._puerto}][BACKUP][replicacion] ALERTA: salto de seq_op "
                f"({subasta.seq_op} -> {estado['seq_op']}), se perdieron actualizaciones intermedias"
            )

        subasta.aplicar_estado_replicado(estado)
        print(
            f"[{self._puerto}][BACKUP][replicacion] Estado replicado: "
            f"mejor_oferta={subasta.mejor_oferta} mejor_postor={subasta.mejor_postor!r} seq_op={subasta.seq_op}"
        )

