"""
Punto de entrada para ejecutar el nodo del sistema de subastas (Primario por defecto).
Soporta tambien el flag --backup para mantener compatibilidad con scripts existentes.

Correr primario:
    python -m nodoPrimario.servidor --host localhost --puerto 9091 --articulo "Cuadro"

Correr backup:
    python -m backup.servidor --puerto 9092
    (o: python -m nodoPrimario.servidor --puerto 9092 --backup)
"""

import argparse
import threading
import time

import Pyro5.api
from comun import config
from nodoPrimario.nodo import NodoSubasta
from nodoPrimario.subasta import DURACION_VENTANA_SEG


def main():
    parser = argparse.ArgumentParser(description="Nodo Primario / Servidor de Subastas")
    parser.add_argument("--articulo", default="Articulo de prueba", help="Nombre del articulo a subastar")
    parser.add_argument("--host", default="localhost", help="Host de escucha")
    parser.add_argument("--puerto", type=int, required=True, help="Puerto de escucha")
    parser.add_argument(
        "--backup",
        action="store_true",
        help="arranca este nodo como backup en vez de primario (compatible con versiones anteriores)",
    )
    args = parser.parse_args()

    es_primario = not args.backup
    nodo = NodoSubasta(
        articulo=args.articulo,
        es_primario=es_primario,
        host=args.host,
        puerto=args.puerto,
    )

    daemon = Pyro5.api.Daemon(host=args.host, port=args.puerto)
    daemon.register(nodo, objectId=config.OBJECT_ID)

    rol = "PRIMARIO" if es_primario else "BACKUP"
    print(f"[{args.puerto}][{rol}] Nodo escuchando en {args.host}:{args.puerto} como {rol}")
    print(f"[{args.puerto}][{rol}] Articulo: {args.articulo!r}, ventana: {DURACION_VENTANA_SEG}s")
    print(f"[{args.puerto}][{rol}] URI: {config.uri_de(args.host, args.puerto)}")

    hilo_daemon = threading.Thread(target=daemon.requestLoop, daemon=True)
    hilo_daemon.start()

    if not es_primario:
        print(f">>> Nodo backup activo en :{args.puerto}, esperando replicacion del primario (Ctrl+C para salir) <<<")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{args.puerto}][BACKUP] Servidor cerrado.")
        return

    print(">>> Presiona 's' + Enter para INICIAR la subasta <<<")
    while True:
        try:
            cmd = input().strip().lower()
            if cmd == "s":
                nodo.iniciar_subasta()
                break
            else:
                print("  Comando no reconocido. Presiona 's' para iniciar la subasta.")
        except (KeyboardInterrupt, EOFError):
            break

    # Mantener el hilo principal activo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{args.puerto}][PRIMARIO] Servidor cerrado.")


if __name__ == "__main__":
    main()
