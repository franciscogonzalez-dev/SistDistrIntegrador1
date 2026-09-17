"""
Punto de entrada para ejecutar el nodo del sistema de subastas (Primario por defecto).
Soporta tambien el flag --backup para mantener compatibilidad con scripts existentes.

Correr primario:
    python -m nodoPrimario.servidor --host <IP_DEL_CONFIG> --puerto 9091

Correr backup:
    python -m backup.servidor --host <IP_DEL_CONFIG> --puerto 9092
    (o: python -m nodoPrimario.servidor --host <IP_DEL_CONFIG> --puerto 9092 --backup)
"""

import argparse
import threading
import time
import json
import random

import Pyro5.api
from comun import config
from nodoPrimario.nodo import NodoSubasta
from nodoPrimario.subasta import DURACION_VENTANA_SEG


def main():
    parser = argparse.ArgumentParser(description="Nodo Primario / Servidor de Subastas")
    parser.add_argument("--autos_json", default="autos.json", help="Ruta al JSON de autos")
    parser.add_argument("--host", default="127.0.0.1", help="Host de escucha")
    parser.add_argument("--puerto", type=int, required=True, help="Puerto de escucha")
    parser.add_argument(
        "--backup",
        action="store_true",
        help="arranca este nodo como backup en vez de primario (compatible con versiones anteriores)",
    )
    args = parser.parse_args()

    es_primario = not args.backup
    
    # Cargar autos si es primario (el backup igual lo inicializa pero recibe despues la replicacion)
    try:
        with open(args.autos_json, "r", encoding="utf-8") as f:
            lista_autos = json.load(f)
            ronda_autos = random.sample(lista_autos, min(3, len(lista_autos)))
    except Exception as e:
        print(f"Error cargando {args.autos_json}: {e}. Se usará un auto por defecto.")
        ronda_autos = [{"marca": "Auto", "modelo": "Por defecto", "anio": 2000, "kilometraje": 0, "fallas_defectos": "", "imagenes": []}]

    nodo = NodoSubasta(
        ronda_autos=ronda_autos,
        es_primario=es_primario,
        host=args.host,
        puerto=args.puerto,
    )

    daemon = Pyro5.api.Daemon(host=args.host, port=args.puerto)
    daemon.register(nodo, objectId=config.OBJECT_ID)

    rol = "PRIMARIO" if nodo.es_primario() else "BACKUP"
    print(f"[{args.puerto}][{rol}] Nodo escuchando en {args.host}:{args.puerto} como {rol}")
    print(f"[{args.puerto}][{rol}] Ronda de {len(ronda_autos)} autos lista, ventana: {DURACION_VENTANA_SEG}s")
    print(f"[{args.puerto}][{rol}] URI: {config.uri_de(args.host, args.puerto)}")

    hilo_daemon = threading.Thread(target=daemon.requestLoop, daemon=True)
    hilo_daemon.start()

    if not nodo.es_primario():
        print(f">>> Nodo réplica activo en :{args.puerto}, sincronizado y vigilando al primario (Ctrl+C para salir) <<<")
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
