"""
Nodo del sistema de subastas. Version "camino feliz" extendida: sigue siendo
un unico nodo actuando de primario, pero ya:
  - escucha en una IP:puerto fija (sin name server, ver comun/config.py)
  - expone es_primario() para que los clientes lo puedan descubrir
  - calcula el monto en el servidor a partir de un incremento (nunca confia
    en un monto absoluto que mande el cliente)
  - reinicia la ventana de 30s con cada oferta aceptada (cierre suave)

Correr:
    python -m primario.servidor --host localhost --puerto 9091 --articulo "Cuadro"
"""

import argparse
import threading
import time

import Pyro5.api

from comun.reloj_lamport import RelojLamport
from comun.protocolo import EstadoSubasta
from comun import config

DURACION_VENTANA_SEG = 30.0


@Pyro5.api.expose
class NodoSubasta:
    def __init__(self, articulo: str):
        self._articulo = articulo
        self._ultimo_evento = time.time()  # se resetea con cada oferta aceptada
        self._mejor_oferta = 0.0
        self._mejor_postor: str | None = None
        self._cerrada = False
        self._reloj = RelojLamport()
        self._lock = threading.Lock()
        # Hardcodeado en True por ahora: todavia no hay eleccion. Cuando se
        # implemente, este valor va a depender del resultado del algoritmo
        # de eleccion en vez de ser fijo.
        self._es_primario = True

    def es_primario(self) -> bool:
        return self._es_primario

    def _tiempo_restante(self) -> float:
        restante = DURACION_VENTANA_SEG - (time.time() - self._ultimo_evento)
        return max(0.0, restante)

    def _estado_actual(self) -> dict:
        return EstadoSubasta(
            articulo=self._articulo,
            mejor_oferta=self._mejor_oferta,
            mejor_postor=self._mejor_postor,
            tiempo_restante_seg=self._tiempo_restante(),
            clock_lamport=self._reloj.valor(),
            cerrada=self._cerrada or self._tiempo_restante() <= 0,
        ).serializar()

    def ofertar(self, cliente_id: str, incremento: float, clock_cliente: int) -> dict:
        """
        Un cliente llama esto para ofertar. Manda el INCREMENTO elegido
        (+50, +100, etc.), nunca un monto absoluto: el monto final siempre
        se calcula aca adentro, con el valor mas actual del servidor, para
        que dos clientes que partieron de la misma lectura no puedan pisarse.
        """
        with self._lock:
            self._reloj.actualizar(clock_cliente)

            if self._cerrada or self._tiempo_restante() <= 0:
                self._cerrada = True
                return {
                    "aceptada": False,
                    "motivo": "La subasta ya cerro",
                    "estado": self._estado_actual(),
                }

            if incremento <= 0:
                return {
                    "aceptada": False,
                    "motivo": "El incremento debe ser positivo",
                    "estado": self._estado_actual(),
                }

            nuevo_monto = self._mejor_oferta + incremento

            self._mejor_oferta = nuevo_monto
            self._mejor_postor = cliente_id
            self._ultimo_evento = time.time()  # reinicia la ventana de 30s
            self._reloj.tick()

            print(f"[nodo] nueva mejor oferta: {cliente_id} -> {nuevo_monto}")

            # ACA es donde en el proximo paso vamos a replicar a los backups
            # antes de (o despues de) responder al cliente.

            return {
                "aceptada": True,
                "motivo": "Oferta aceptada",
                "estado": self._estado_actual(),
            }

    def obtener_estado(self) -> dict:
        with self._lock:
            return self._estado_actual()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--articulo", default="Articulo de prueba")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--puerto", type=int, required=True)
    args = parser.parse_args()

    nodo = NodoSubasta(args.articulo)

    daemon = Pyro5.api.Daemon(host=args.host, port=args.puerto)
    daemon.register(nodo, objectId=config.OBJECT_ID)

    print(f"Nodo escuchando en {args.host}:{args.puerto}")
    print(f"Articulo: {args.articulo!r}, ventana: {DURACION_VENTANA_SEG}s")
    print(f"URI: {config.uri_de(args.host, args.puerto)}")

    daemon.requestLoop()


if __name__ == "__main__":
    main()
