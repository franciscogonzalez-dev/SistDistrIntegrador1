# run python -m cliente.cliente --id (pablo rosales x ej)

import argparse

import Pyro5.api

from comun.reloj_lamport import RelojLamport


def mostrar_estado(estado: dict):
    print(
        f"  articulo={estado['articulo']!r} "
        f"mejor_oferta={estado['mejor_oferta']} "
        f"mejor_postor={estado['mejor_postor']} "
        f"tiempo_restante={estado['tiempo_restante_seg']:.1f}s "
        f"cerrada={estado['cerrada']}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="identificador del cliente")
    parser.add_argument("--ip", required=True, help="IP del servidor")
    parser.add_argument("--puerto", type=int, default=9002)
    args = parser.parse_args()

    reloj = RelojLamport()

    primario = Pyro5.api.Proxy(
        f"PYRO:nodo_primario@{args.ip}:{args.puerto}"
    )

    print(f"Cliente {args.id!r} conectado. Estado inicial:")
    mostrar_estado(primario.obtener_estado())

    while True:
        entrada = input(f"[{args.id}] monto a ofertar (o 'q' para salir): ")
        if entrada.strip().lower() == "q":
            break
        try:
            monto = float(entrada)
        except ValueError:
            print("  monto invalido, probá de nuevo")
            continue

        clock_envio = reloj.tick()  # incrementamos ANTES de mandar el mensaje
        respuesta = primario.ofertar(args.id, monto, clock_envio)

        reloj.actualizar(respuesta["estado"]["clock_lamport"])  # sincronizamos

        print(f"  {respuesta['motivo']}")
        mostrar_estado(respuesta["estado"])


if __name__ == "__main__":
    main()
