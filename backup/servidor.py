"""
Punto de entrada para ejecutar un nodo en modo BACKUP.

Correr:
    python -m backup.servidor --host <IP_DEL_CONFIG> --puerto 9092
    python -m backup.servidor --host <IP_DEL_CONFIG> --puerto 9093
"""

import argparse
import json
import threading
import time

import Pyro5.api
from comun import config
from nodoPrimario.nodo import NodoSubasta


def main():
    parser = argparse.ArgumentParser(description="Nodo Backup del sistema de subastas")
    parser.add_argument("--puerto", type=int, required=True, help="Puerto donde escucha este nodo backup")
    parser.add_argument("--host", default="127.0.0.1", help="Host donde escucha este nodo backup")
    parser.add_argument("--autos_json", default="autos.json", help="JSON opcional para inicializar la ronda local del backup")
    args = parser.parse_args()

    try:
        with open(args.autos_json, "r", encoding="utf-8") as f:
            lista_autos = json.load(f)
            ronda_autos = lista_autos[:3] if len(lista_autos) >= 3 else lista_autos
    except Exception:
        ronda_autos = [{"marca": "Auto", "modelo": "Backup", "anio": 2000, "kilometraje": 0, "fallas_defectos": "", "imagenes": []}]

    nodo = NodoSubasta(
        ronda_autos=ronda_autos,
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
