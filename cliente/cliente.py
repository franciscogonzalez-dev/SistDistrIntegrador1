"""
Cliente de consola. No usa name server: conoce de antemano la lista fija de
nodos (comun/config.py) y los recorre preguntando "sos vos el primario?"
hasta encontrar al que responda que si. Si despues una llamada falla porque
el nodo que tenia guardado se cayo, vuelve a recorrer la lista.

Correr:
    python -m cliente.cliente --id ana
"""

import threading
import argparse

import Pyro5.api
import Pyro5.errors

from comun.reloj_lamport import RelojLamport
from comun import config

TIMEOUT_DESCUBRIMIENTO_SEG = 1.5


OPCIONES_OFERTA = {
    "1": 100.0,
    "2": 250.0,
    "3": 500.0,
}

def encontrar_primario():
    """Recorre config.NODOS y devuelve un Proxy al que dice ser primario."""
    for host, puerto in config.NODOS:
        proxy = Pyro5.api.Proxy(config.uri_de(host, puerto))
        proxy._pyroTimeout = TIMEOUT_DESCUBRIMIENTO_SEG
        try:
            if proxy.es_primario():
                print(f"  primario encontrado en {host}:{puerto}")
                return proxy
        except Pyro5.errors.CommunicationError:
            continue  # este nodo no responde, probamos el siguiente
    raise RuntimeError("Ningun nodo de la lista respondio como primario")


def mostrar_estado(estado: dict):
    print(
        f"  articulo={estado['articulo']!r} "
        f"mejor_oferta={estado['mejor_oferta']} "
        f"mejor_postor={estado['mejor_postor']} "
        f"tiempo_restante={estado['tiempo_restante_seg']:.1f}s "
        f"secuencia_operacion={estado['seq_op']} " #debug, nose si lo dejamos para la muestra
        f"cerrada={estado['cerrada']}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="identificador del cliente")
    args = parser.parse_args()

    reloj = RelojLamport()

    print("Buscando nodo primario...")
    primario = encontrar_primario()

    print(f"Cliente {args.id!r} conectado. Estado inicial:")
    estado_inicial = primario.obtener_estado()
    seq_visto  = estado_inicial["seq_op"] #nro de operacion que se ve al conectarse
    mostrar_estado(estado_inicial)

    while True:
        entrada = input(f"[{args.id}] incremento a ofertar [1] +100 [2] +250 [3] +500 (o 'q' para salir): ").strip()
        if entrada.lower() == "q":
            break
        if entrada not in OPCIONES_OFERTA:
            print("  opcion invalida, proba de nuevo (opciones validas: 1, 2, 3 o q)")
            continue

        incremento = OPCIONES_OFERTA[entrada]

        clock_envio = reloj.tick()
        try:
            respuesta = primario.ofertar(args.id, incremento, clock_envio, seq_visto)
        except Pyro5.errors.CommunicationError:
            print("  el nodo dejo de responder, buscando nuevo primario...")
            primario = encontrar_primario()
            continue

        reloj.actualizar(respuesta["estado"]["clock_lamport"])
        seq_visto = respuesta["estado"]["seq_op"] 

        print(f"  {respuesta['motivo']}")
        mostrar_estado(respuesta["estado"])


if __name__ == "__main__":
    main()
