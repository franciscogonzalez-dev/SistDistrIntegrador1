"""
Cliente de consola. No usa name server: conoce de antemano la lista fija de
nodos (comun/config.py) y los recorre preguntando "sos vos el primario?"
hasta encontrar al que responda que si. Si despues una llamada falla porque
el nodo que tenia guardado se cayo, vuelve a recorrer la lista.

Correr:
    python -m cliente.cliente --id ana
"""

import argparse
import threading

import Pyro5.api
import Pyro5.errors

from comun.reloj_lamport import RelojLamport
from comun import config

TIMEOUT_DESCUBRIMIENTO_SEG = 1.5


def encontrar_primario():
    """Recorre config.NODOS y devuelve un Proxy al que dice ser primario."""
    try: 
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
    
    except RuntimeError as e: #agregue esto porque capaz no le copa al profe que salga toda la excep
        print(f"Error al descubrir primario: {e}")
        exit(1)

def mostrar_estado(estado: dict):
    print(
        f"  articulo={estado['articulo']!r} "
        f"mejor_oferta={estado['mejor_oferta']} "
        f"mejor_postor={estado['mejor_postor']} "
        f"tiempo_restante={estado['tiempo_restante_seg']:.1f}s "
        f"secuencia_operacion={estado['seq_op']} " #debug, nose si lo dejamos para la muestra
        f"cerrada={estado['cerrada']}"
    )

@Pyro5.api.expose
class ClienteCallback: #para que el server le pueda avisar al cliente cuando la subasta se cierra o cuando hay una nueva mejor oferta
    def notificar_estado(self, estado: dict):
        print(f"\n  [AVISO!] estado actualizado: mejor_oferta={estado['mejor_oferta']} "
              f"cerrada={estado['cerrada']}")
        
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="identificador del cliente")
    args = parser.parse_args()

    reloj = RelojLamport()

    print("Buscando nodo primario...")
    primario = encontrar_primario()
    
    daemon_cliente = Pyro5.api.Daemon() #daemon para que el server pueda llamar al cliente y avisarle cuando la subasta se cierra
    callback = ClienteCallback()
    uri_callback = daemon_cliente.register(callback)
    threading.Thread(target=daemon_cliente.requestLoop, daemon=True).start()
    primario.subscribir_cliente(str(uri_callback))


    print(f"Cliente {args.id!r} conectado. Estado inicial:")
    estado_inicial = primario.obtener_estado()
    seq_visto  = estado_inicial["seq_op"] #nro de operacion que se ve al conectarse
    mostrar_estado(estado_inicial)

    while True:
        entrada = input(f"[{args.id}] incremento a ofertar (o 'q' para salir): ")
        if entrada.strip().lower() == "q":
            break
        try:
            incremento = float(entrada)
        except ValueError:
            print("  incremento invalido, proba de nuevo")
            continue

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
