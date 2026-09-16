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

def formatear_moneda(monto) -> str:
    """Formatea valores monetarios con separador de miles."""
    try:
        return f"${float(monto):,.0f}"
    except (ValueError, TypeError):
        return f"${monto}"

def mostrar_estado(estado: dict):
    tiempo = max(0.0, estado.get("tiempo_restante_seg", 0.0))
    print(
        f"  articulo={formatear_articulo(estado.get('articulo'))} | "
        f"mejor_oferta={formatear_moneda(estado.get('mejor_oferta', 0))} | "
        f"mejor_postor={estado.get('mejor_postor')} | "
        f"tiempo_restante={tiempo:.1f}s | "
        f"secuencia={estado.get('seq_op', 0)} | "
        f"cerrada={estado.get('cerrada', False)}"
    )

@Pyro5.api.expose
class ClienteCallback: # para que el server le pueda avisar al cliente cuando la subasta se cierra o cuando hay una nueva mejor oferta
    def __init__(self, estado_local: dict, cliente_id: str = ""):
        self.estado_local = estado_local
        self.cliente_id = cliente_id

    def notificar_estado(self, estado: dict):
        # Separador visual para no romper la linea del input()
        print("\n" + "─" * 65)

        # 1. Si el auto actual cerro, anunciar si este cliente gano o perdio
        if estado.get("cerrada") and not estado.get("ronda_finalizada"):
            ganador = estado.get("mejor_postor")
            if ganador and ganador.strip().lower() == self.cliente_id.strip().lower():
                print(f"  [¡GANASTE LA SUBASTA!] Te adjudicaste el {formatear_articulo(estado.get('articulo'))} por {formatear_moneda(estado.get('mejor_oferta', 0))}.")
            elif ganador:
                print(f"  [SUBASTA FINALIZADA] El {formatear_articulo(estado.get('articulo'))} fue ganado por '{ganador}' por {formatear_moneda(estado.get('mejor_oferta', 0))}.")
            else:
                print(f"  [SUBASTA FINALIZADA] El {formatear_articulo(estado.get('articulo'))} cerro sin ofertas.")

        # 2. Si la ronda completa de autos termino
        if estado.get("ronda_finalizada"):
            ganador = estado.get("mejor_postor")
            if ganador and ganador.strip().lower() == self.cliente_id.strip().lower():
                print(f"  [¡GANASTE LA SUBASTA!] Te adjudicaste el {formatear_articulo(estado.get('articulo'))} por {formatear_moneda(estado.get('mejor_oferta', 0))}.")
            elif ganador:
                print(f"  [SUBASTA FINALIZADA] El {formatear_articulo(estado.get('articulo'))} fue ganado por '{ganador}' por {formatear_moneda(estado.get('mejor_oferta', 0))}.")
            print("  [RONDA FINALIZADA] No quedan mas autos para subastar. Gracias por participar.")

        # 3. Si la subasta sigue abierta (en curso o pasando a un nuevo auto)
        elif not estado.get("cerrada"):
            postor = estado.get("mejor_postor")
            if postor is None:
                print(f"  [SIGUIENTE AUTO EN SUBASTA] {formatear_articulo(estado.get('articulo'))} | Base: {formatear_moneda(estado.get('mejor_oferta', 0))}")
            elif postor.strip().lower() == self.cliente_id.strip().lower():
                print(f"  [AVISO] ¡Vas ganando! Tu oferta de {formatear_moneda(estado.get('mejor_oferta', 0))} es la mejor actual.")
            else:
                print(f"  [AVISO] Nueva mejor oferta: {formatear_moneda(estado.get('mejor_oferta', 0))} por '{postor}' (tiempo: {max(0.0, estado.get('tiempo_restante_seg', 0.0)):.1f}s)")

        print("─" * 65)
        self.estado_local["reloj"].actualizar(estado["clock_lamport"])
        self.estado_local["seq_visto"] = estado["seq_op"]
        # Re-imprimir el prompt para mantener el cursor en su lugar
        print(f"  {self.cliente_id.upper()} > ", end="", flush=True)

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
    callback = ClienteCallback(estado_local, cliente_id=args.id)
    uri_callback = daemon_cliente.register(callback)
    threading.Thread(target=daemon_cliente.requestLoop, daemon=True).start()
    primario.subscribir_cliente(str(uri_callback))

    print(f"Cliente {args.id!r} conectado. Estado inicial:")
    estado_inicial = primario.obtener_estado()
    estado_local["seq_visto"] = estado_inicial["seq_op"]
    mostrar_estado(estado_inicial)

    try:
        if not estado_inicial.get("iniciada"):
            print("  Esperando inicio de la subasta...")
            while not primario.obtener_estado().get("iniciada"):
                time.sleep(1.0)
            print("  Subasta iniciada.")

        while True:
            print("\n  [1] +$100 | [2] +$250 | [3] +$500 | [q] Salir")
            entrada = input(f"  {args.id.upper()} > ").strip()
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

    except (KeyboardInterrupt, EOFError):
        print("\n  Saliendo...")
    finally:
        print("  Cerrando conexion y liberando recursos...")
        try:
            if primario:
                primario._pyroClaimOwnership()
                primario.desubscribir_cliente(str(uri_callback))
        except Exception:
            pass
        daemon_cliente.shutdown()
        print("  ¡Conexion cerrada limpiamente!")


if __name__ == "__main__":
    main()
