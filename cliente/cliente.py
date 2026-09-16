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

TIMEOUT_DESCUBRIMIENTO_SEG = 0.8
TIMEOUT_OPERACION_SEG = 6.0


OPCIONES_OFERTA = {
    "1": 100.0,
    "2": 250.0,
    "3": 500.0,
}

def encontrar_primario():
    """Busca el nodo primario activo en el cluster."""
    # 1. Intentar encontrar un nodo que afirme ser primario y responda
    for host, puerto in config.NODOS:
        nodo = Pyro5.api.Proxy(config.uri_de(host, puerto))
        nodo._pyroTimeout = TIMEOUT_DESCUBRIMIENTO_SEG
        try:
            if nodo.es_primario():
                nodo.obtener_estado()
                nodo._pyroTimeout = TIMEOUT_OPERACION_SEG
                return nodo
        except Exception:
            continue

    # 2. Si ninguno afirmo ser primario de forma directa, consultar a los vivos quien es el primario
    for host, puerto in config.NODOS:
        nodo = Pyro5.api.Proxy(config.uri_de(host, puerto))
        nodo._pyroTimeout = TIMEOUT_DESCUBRIMIENTO_SEG
        try:
            ubicacion = nodo.ubicacion_primario()
            primario = Pyro5.api.Proxy(config.uri_de(ubicacion["host"], ubicacion["puerto"]))
            primario._pyroTimeout = TIMEOUT_DESCUBRIMIENTO_SEG
            if primario.es_primario():
                primario.obtener_estado()
                primario._pyroTimeout = TIMEOUT_OPERACION_SEG
                return primario
        except Exception:
            continue

    raise RuntimeError("Ningun nodo del cluster pudo indicar el primario")

def reconectar_con_reintentos(intentos: int = 10, espera_seg: float = 0.5):
    """
    Reintenta ubicar un primario ante fallas transitorias (por ej. una
    eleccion todavia en curso justo despues de que el primario se cayo).
    Devuelve None si se agotaron los intentos.
    """
    for intento in range(1, intentos + 1):
        try:
            return encontrar_primario()
        except RuntimeError:
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
        f"secuencia_operacion={estado['seq_op']} "
        f"cerrada={estado['cerrada']}"
    )

@Pyro5.api.expose
class ClienteCallback: # para que el server le pueda avisar al cliente cuando la subasta se cierra o cuando hay una nueva mejor oferta
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
    primario = reconectar_con_reintentos()
    if primario is None:
        print("No se encontro ningun nodo primario disponible.")
        return
    
    daemon_cliente = Pyro5.api.Daemon()
    callback = ClienteCallback(estado_local)
    uri_callback = daemon_cliente.register(callback)
    threading.Thread(target=daemon_cliente.requestLoop, daemon=True).start()
    primario.subscribir_cliente(str(uri_callback))

    print(f"Cliente {args.id!r} conectado. Estado inicial:")
    estado_inicial = primario.obtener_estado()
    estado_local["seq_visto"] = estado_inicial["seq_op"]
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
            if primario:
                primario._pyroClaimOwnership()
            respuesta = primario.ofertar(args.id, incremento, clock_envio, estado_local["seq_visto"])
            if not respuesta.get("aceptada") and "backup" in respuesta.get("motivo", "").lower():
                raise Pyro5.errors.CommunicationError("Nodo respondio como backup")
        except (Pyro5.errors.CommunicationError, Pyro5.errors.PyroError):
            print("  el primario no respondio o cambio de rol, buscando nuevo primario...")
            nuevo_primario = reconectar_con_reintentos()
            if nuevo_primario is None:
                print("  no se pudo ubicar un primario disponible, proba de nuevo en unos segundos.")
                continue
            primario = nuevo_primario

            try:
                primario._pyroClaimOwnership()
                primario.subscribir_cliente(str(uri_callback))
                # Sincronizar estado y reintentar la oferta automaticamente
                nuevo_estado = primario.obtener_estado()
                estado_local["seq_visto"] = nuevo_estado["seq_op"]
                reloj.actualizar(nuevo_estado["clock_lamport"])
                clock_envio = reloj.tick()
                respuesta = primario.ofertar(args.id, incremento, clock_envio, estado_local["seq_visto"])
            except Exception as e:
                print(f"  error al reintentar oferta en nuevo primario: {e}")
                continue

        reloj.actualizar(respuesta["estado"]["clock_lamport"])
        estado_local["seq_visto"] = respuesta["estado"]["seq_op"] 

        print(f"  {respuesta['motivo']}")
        mostrar_estado(respuesta["estado"])


if __name__ == "__main__":
    main()
