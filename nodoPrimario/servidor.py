"""
Nodo del sistema de subastas. Version "camino feliz" extendida: sigue siendo
un unico nodo actuando de primario, pero ya:
  - escucha en una IP:puerto fija (sin name server, ver comun/config.py)
  - expone es_primario() para que los clientes lo puedan descubrir
  - calcula el monto en el servidor a partir de un incremento (nunca confia
    en un monto absoluto que mande el cliente)
  - reinicia la ventana de 30s con cada oferta aceptada (cierre suave)

Correr:
    python -m primario.servidor --host localhost --puerto 9091 --articulo "Cuadro"
    
    a mi me funciona con: 
        python -m nodoPrimario.servidor --host localhost --puerto 9091 --articulo "Cuadro"
"""

import argparse
import threading
import time

import Pyro5.api

from comun.reloj_lamport import RelojLamport
from comun.protocolo import EstadoSubasta
from comun import config

DURACION_VENTANA_SEG = 30.0


@Pyro5.api.expose
class NodoSubasta:
    def __init__(self, articulo: str):
        self._articulo = articulo
        self._ultimo_evento = time.time()  # se resetea con cada oferta aceptada
        self._mejor_oferta = 0.0
        self._mejor_postor: str | None = None
        self._cerrada = False
        self._iniciada = False
        self._reloj = RelojLamport()
        self._lock = threading.Lock()
        self._seq_op= 0 
        # Hardcodeado en True por ahora: todavia no hay eleccion. Cuando se
        # implemente, este valor va a depender del resultado del algoritmo
        # de eleccion en vez de ser fijo.
        self._es_primario = True
        
        #esto permite informar a los clientes cuando la subasta se cierra, el server les pushea la informacion
        self._clientes = []
        threading.Thread(target=self._vigila_cierre, daemon=True).start() # thread aparte ya que el requestLoop de Pyro es bloqueante y se queda atendiendo clientes
        self._ganador_anunciado = False #flag para que solo se notifique una vez el cierre de la subasta, si se hacen varias rondas se deberia resetear esta flag

    def iniciar_subasta(self) -> bool:
        """El operador del servidor llama a esto presionando 's' para arrancar la subasta."""
        with self._lock:
            if self._iniciada:
                print("[nodo] La subasta ya estaba iniciada.")
                return False
            self._iniciada = True
            self._ultimo_evento = time.time()
            print(f"[nodo] ¡SUBASTA INICIADA! Articulo: {self._articulo!r}, ventana de {DURACION_VENTANA_SEG}s activa.")
            self._notificar_clientes()
            return True

    def es_primario(self) -> bool:
        return self._es_primario

    def _tiempo_restante(self) -> float:
        if not self._iniciada:
            return DURACION_VENTANA_SEG
        restante = DURACION_VENTANA_SEG - (time.time() - self._ultimo_evento)
        return max(0.0, restante)

    def _estado_actual(self) -> dict:
        return EstadoSubasta(
            articulo=self._articulo,
            mejor_oferta=self._mejor_oferta,
            mejor_postor=self._mejor_postor,
            tiempo_restante_seg=self._tiempo_restante(),
            clock_lamport=self._reloj.valor(),
            seq_op=self._seq_op,
            cerrada=self._cerrada or (self._iniciada and self._tiempo_restante() <= 0),
            iniciada=self._iniciada,
        ).serializar()

    def ofertar(self, cliente_id: str, incremento: float, clock_cliente: int, seq_op_cliente: int) -> dict:
        """
        Un cliente llama esto para ofertar. Manda el INCREMENTO elegido
        (+50, +100, etc.), nunca un monto absoluto: el monto final siempre
        se calcula aca adentro, con el valor mas actual del servidor, para
        que dos clientes que partieron de la misma lectura no puedan pisarse.
        """
        time.sleep(0.5) #para poder simular ofertas simultaneas
        with self._lock:
            self._reloj.actualizar(clock_cliente)

            if not self._iniciada:
                return {
                    "aceptada": False,
                    "motivo": "La subasta aun no ha iniciado. Esperando a que el servidor la inicie...",
                    "estado": self._estado_actual(),
                }

            if self._cerrada or self._tiempo_restante() <= 0:
                self._cerrada = True
                return {
                    "aceptada": False,
                    "motivo": "La subasta ya cerro",
                    "estado": self._estado_actual(),
                }

            if incremento <= 0:
                return {
                    "aceptada": False,
                    "motivo": "El incremento debe ser positivo",
                    "estado": self._estado_actual(),
                }
            
            #validar que el cliente no tenga un estado desactualizado
            if seq_op_cliente != self._seq_op:
                return {
                    "aceptada": False,
                    "motivo": ( 
                        f"El monto cambio a {self._mejor_oferta} antes de procesar tu oferta, volve a intentar"
                    ),
                    "estado": self._estado_actual(),
                }


            nuevo_monto = self._mejor_oferta + incremento

            self._mejor_oferta = nuevo_monto
            self._mejor_postor = cliente_id
            self._ultimo_evento = time.time()  # reinicia la ventana de 30s
            self._reloj.tick()
            self._seq_op += 1

            print(f"[nodo] nueva mejor oferta: {cliente_id} -> {nuevo_monto}")

            # ACA es donde en el proximo paso vamos a replicar a los backups
            # antes de (o despues de) responder al cliente.

            self._notificar_clientes() #avisar a los clientes que la subasta se cerro
            return {
                "aceptada": True,
                "motivo": "Oferta aceptada",
                "estado": self._estado_actual(),
            }

    def obtener_estado(self) -> dict:
        with self._lock:
            return self._estado_actual()
        
    ### agregue esto para manejar laa notificacion del cierre
    def subscribir_cliente(self, uri_cliente: str):
        proxy = Pyro5.api.Proxy(uri_cliente) # crear proxy para comunicarse con el cliente
        proxy._pyroOneway.add("notificar_estado") # el metodo "notificar_estado" esta en el cliente y es asincrono, el server envia notificacion al cliente y no espera respuesta
        with self._lock:
            self._clientes.append(proxy)
            print(f"[nodo] un cliente se unió a la sala: {uri_cliente}")
    
    def _notificar_clientes(self):
        estado = self._estado_actual()
        vivos = []
        for proxy in self._clientes:
            try:
                proxy._pyroClaimOwnership() #importante ya que a lista de clientes es tocada por varios hilos, Pyro tira error si no se le dice explicatmente que hilo es el que esta tocando al proxy en el momento
                                    #los hilos que llaman a los obj Proxy de cada cliente ocurre desde ofertar() y desde _vigila_cierre() que son hilos distintos, por eso hay que reclamar la propiedad del proxy antes de usarlo
                proxy.notificar_estado(estado)
                vivos.append(proxy)
            except Pyro5.errors.CommunicationError:
                pass  # ese cliente se cayo, no lo volvemos a agregar
        self._clientes = vivos #esto capaz esta de mas pero sirve para quedarnos solo con los clientes que siguen vivos
    
    def _vigila_cierre(self):
        while True:
            time.sleep(1.0)
            with self._lock:
                if self._iniciada and not self._ganador_anunciado and self._tiempo_restante() <= 0: #aca en caso de hacer varias rondas se deberia resetear la flag 
                    self._cerrada = True
                    self._ganador_anunciado = True
                    print(f"[nodo] SUBASTA CERRADA. Ganador: {self._mejor_postor!r} con {self._mejor_oferta}")
                    self._notificar_clientes()  # mismo metodo que usarian tras una oferta aceptada


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--articulo", default="Articulo de prueba")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--puerto", type=int, required=True)
    args = parser.parse_args()

    nodo = NodoSubasta(args.articulo)

    daemon = Pyro5.api.Daemon(host=args.host, port=args.puerto)
    daemon.register(nodo, objectId=config.OBJECT_ID)

    print(f"Nodo escuchando en {args.host}:{args.puerto}")
    print(f"Articulo: {args.articulo!r}, ventana: {DURACION_VENTANA_SEG}s")
    print(f"URI: {config.uri_de(args.host, args.puerto)}")

    # Cambié daemon.requestLoop() por un hilo,  esto es para que el hilo principal no espere a que terminen todas las llamadas de los clientes y pueda ejecutar el comando 's' para iniciar la subasta
    hilo_daemon = threading.Thread(target=daemon.requestLoop, daemon=True)
    hilo_daemon.start()

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
        print("\nServidor cerrado.")


if __name__ == "__main__":
    main()
