"""
Cliente de consola. Conoce una lista fija de nodos de arranque, pero la
eleccion del primario la hacen los propios nodos. Si el nodo guardado cae,
consulta a otro nodo y sigue la ubicacion del nuevo primario.

Correr:
    python -m cliente.cliente --id ana
"""

import argparse
import threading
import time

import Pyro5.api
import Pyro5.errors

from comun.reloj_lamport import RelojLamport
from comun import config

TIMEOUT_DESCUBRIMIENTO_SEG = 1.5
TIMEOUT_OPERACION_SEG = 6.0


OPCIONES_OFERTA = {
    "1": 100.0,
    "2": 250.0,
    "3": 500.0,
}

def encontrar_primario():
    """Pide a un nodo vivo la ubicacion decidida por el cluster."""
    for host, puerto in config.NODOS:
        nodo = Pyro5.api.Proxy(config.uri_de(host, puerto))
        nodo._pyroTimeout = TIMEOUT_DESCUBRIMIENTO_SEG
        try:
            ubicacion = nodo.ubicacion_primario()
            primario = Pyro5.api.Proxy(config.uri_de(ubicacion["host"], ubicacion["puerto"]))
            primario._pyroTimeout = TIMEOUT_DESCUBRIMIENTO_SEG
            primario.obtener_estado()
            primario._pyroTimeout = TIMEOUT_OPERACION_SEG
            print(f"  primario indicado por el cluster en {ubicacion['host']}:{ubicacion['puerto']}")
            return primario
        except Pyro5.errors.CommunicationError:
            continue
    raise RuntimeError("Ningun nodo del cluster pudo indicar el primario")

def reconectar_con_reintentos(intentos: int = 5, espera_seg: float = 1.0):
    """
    Reintenta ubicar un primario ante fallas transitorias (por ej. una
    eleccion todavia en curso justo despues de que el primario se cayo).
    Devuelve None si se agotaron los intentos, en vez de dejar propagar el
    RuntimeError de encontrar_primario() y tirar abajo el cliente.
    """
    for intento in range(1, intentos + 1):
        try:
            return encontrar_primario()
        except RuntimeError:
            print(f"  no se encontro primario todavia (intento {intento}/{intentos}), reintentando...")
            time.sleep(espera_seg)
    return None

def formatear_articulo(articulo) -> str:
    if isinstance(articulo, dict):
        marca = articulo.get("marca", "-")
        modelo = articulo.get("modelo", "-")
        anio = articulo.get("anio", "-")
        return f"{marca} {modelo} ({anio})"
    return str(articulo)

def mostrar_estado(estado: dict):
    print(
        f"  articulo={formatear_articulo(estado['articulo'])} "
        f"mejor_oferta={estado['mejor_oferta']} "
        f"mejor_postor={estado['mejor_postor']} "
        f"tiempo_restante={estado['tiempo_restante_seg']:.1f}s "
        f"secuencia_operacion={estado['seq_op']} " #debug, nose si lo dejamos para la muestra
        f"cerrada={estado['cerrada']}"
    )

@Pyro5.api.expose
class ClienteCallback: #para que el server le pueda avisar al cliente cuando la subasta se cierra o cuando hay una nueva mejor oferta
    def __init__(self, estado_local: dict):
        self.estado_local = estado_local

    def notificar_estado(self, estado: dict):
        if estado.get("ronda_finalizada"):
            print("\n  [RONDA FINALIZADA] No quedan mas autos para subastar. ¡Gracias por participar!")
        elif estado["cerrada"]:
            print(f"\n  [SUBASTA FINALIZADA] articulo={formatear_articulo(estado['articulo'])} "
                  f"ganador={estado['mejor_postor']!r} monto_final={estado['mejor_oferta']}")
        else:
            print(f"\n  [AVISO!] estado actualizado: mejor_oferta={estado['mejor_oferta']} "
                  f"cerrada={estado['cerrada']}")
        self.estado_local["reloj"].actualizar(estado["clock_lamport"])
        self.estado_local["seq_visto"] = estado["seq_op"]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="identificador del cliente")
    args = parser.parse_args()

    reloj = RelojLamport()
    estado_local = {"reloj": reloj, "seq_visto": 0}

    print("Buscando nodo primario...")
    primario = encontrar_primario()
    
    daemon_cliente = Pyro5.api.Daemon() #daemon para que el server pueda llamar al cliente y avisarle cuando la subasta se cierra
    callback = ClienteCallback(estado_local)
    uri_callback = daemon_cliente.register(callback)
    threading.Thread(target=daemon_cliente.requestLoop, daemon=True).start()
    primario.subscribir_cliente(str(uri_callback))


    print(f"Cliente {args.id!r} conectado. Estado inicial:")
    estado_inicial = primario.obtener_estado()
    estado_local["seq_visto"] = estado_inicial["seq_op"] #nro de operacion que se ve al conectarse
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
            
            respuesta = primario.ofertar(args.id, incremento, clock_envio, estado_local["seq_visto"])
        except Pyro5.errors.CommunicationError:
            print("  el nodo dejo de responder, buscando nuevo primario...")
            nuevo_primario = reconectar_con_reintentos()
            if nuevo_primario is None:
                print("  no se pudo ubicar un primario disponible, proba de nuevo en unos segundos.")
                continue
            primario = nuevo_primario

            try:
                primario.subscribir_cliente(str(uri_callback))
                respuesta = primario.ofertar(args.id, incremento, clock_envio, estado_local["seq_visto"])
            except Pyro5.errors.CommunicationError:
                print("  el nuevo primario tampoco respondio, proba de nuevo en unos segundos.")
                continue

            reloj.actualizar(respuesta["estado"]["clock_lamport"])
            estado_local["seq_visto"] = respuesta["estado"]["seq_op"]
            print(f"  {respuesta['motivo']}")
            mostrar_estado(respuesta["estado"])
            continue

        reloj.actualizar(respuesta["estado"]["clock_lamport"])
        estado_local["seq_visto"] = respuesta["estado"]["seq_op"] 

        print(f"  {respuesta['motivo']}")
        mostrar_estado(respuesta["estado"])


if __name__ == "__main__":
    main()
