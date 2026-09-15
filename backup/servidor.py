"""
Punto de entrada para ejecutar un nodo en modo BACKUP.

Correr:
    python -m backup.servidor --puerto 9092
    python -m backup.servidor --puerto 9093
"""

import argparse
import threading
import time

import Pyro5.api
from comun import config
from nodoPrimario.nodo import NodoSubasta


def main():
    parser = argparse.ArgumentParser(description="Nodo Backup del sistema de subastas")
    parser.add_argument("--puerto", type=int, required=True, help="Puerto donde escucha este nodo backup")
    parser.add_argument("--host", default="localhost", help="Host donde escucha este nodo backup")
    parser.add_argument("--articulo", default="Articulo de prueba", help="Articulo de la subasta")
    args = parser.parse_args()

    nodo = NodoSubasta(
        articulo=args.articulo,
        es_primario=False,
        host=args.host,
        puerto=args.puerto,
    )

    daemon = Pyro5.api.Daemon(host=args.host, port=args.puerto)
    daemon.register(nodo, objectId=config.OBJECT_ID)

    print(f"[{args.puerto}][BACKUP] Escuchando en {args.host}:{args.puerto}")
    print(f"[{args.puerto}][BACKUP] URI: {config.uri_de(args.host, args.puerto)}")
    print(f">>> Nodo BACKUP activo en :{args.puerto}, esperando replicacion y vigilando al primario (Ctrl+C para salir) <<<")

    hilo_daemon = threading.Thread(target=daemon.requestLoop, daemon=True)
    hilo_daemon.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{args.puerto}][BACKUP] Servidor cerrado por el usuario.")


if __name__ == "__main__":
    main()
