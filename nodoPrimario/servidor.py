#nodo primario que expone sus metodos via pyro5 para que los clientes lo usen como locales
"""
Correr (en terminales separadas):
    1) pyro5-ns
    2) python -m primario.servidor --articulo "Cuadro" --duracion 120
"""

import argparse
import threading
import time

import Pyro5.api

from comun.reloj_lamport import RelojLamport
from comun.protocolo import EstadoSubasta


@Pyro5.api.expose
class Primario:
    def __init__(self, articulo: str, duracion_seg: float): # por ahora no inicializamos un valor base de la subasta
        self._articulo = articulo
        self._duracion_seg = duracion_seg
        self._inicio = time.time()
        self._mejor_oferta = 0.0
        self._mejor_postor: str | None = None
        self._cerrada = False
        self._reloj = RelojLamport()
        self._lock = threading.Lock() # bloqueo para que no se transpapelen las ofertas

    def _tiempo_restante(self) -> float:
        restante = self._duracion_seg - (time.time() - self._inicio)
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

    def ofertar(self, cliente_id: str, monto: float, clock_cliente: int) -> dict:
        """
        Un cliente llama esto para ofertar. Devuelve un dict con
        {aceptada, motivo, estado}.
        """
        with self._lock:
            # Sincronizamos nuestro reloj con el del cliente (regla de Lamport)
            self._reloj.actualizar(clock_cliente)

            if self._cerrada or self._tiempo_restante() <= 0: #si ya terminó la subasta no se aceptan mas ofertas
                self._cerrada = True
                return {
                    "aceptada": False,
                    "motivo": "La subasta ya cerro",
                    "estado": self._estado_actual(),
                }

            if monto <= self._mejor_oferta: # si el monto ofertado no supera a la mejor oferta se rechaza
                return {
                    "aceptada": False,
                    "motivo": f"Debe superar la oferta actual ({self._mejor_oferta})",
                    "estado": self._estado_actual(),
                }

            self._mejor_oferta = monto # si se acepta se actualiza la mejor oferta 
            self._mejor_postor = cliente_id #
            self._reloj.tick()  # evento local: aceptamos la oferta

            print(f"[primario] nueva mejor oferta: {cliente_id} -> {monto}") # se informa la nueva mejor oferta

            # aca es donde en el proximo paso vamos a replicar a los backups
            # antes o despues de responder al cliente.

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
    parser.add_argument("--duracion", type=float, default=120.0)
    args = parser.parse_args()

    primario = Primario(args.articulo, args.duracion)

    daemon = Pyro5.api.Daemon()
    ns = Pyro5.api.locate_ns()
    uri = daemon.register(primario, "nodo_primario")
    ns.register("subasta.primario", uri)

    print(f"Primario listo. Articulo: {args.articulo!r}, duracion: {args.duracion}s")
    print(f"Registrado en el name server como 'subasta.primario' -> {uri}")

    daemon.requestLoop()


if __name__ == "__main__":
    main()
