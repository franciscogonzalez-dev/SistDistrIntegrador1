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
TIMEOUT_REPLICACION_SEG = 1.5  # cuanto espera el primario a que un backup confirme antes de saltearlo


@Pyro5.api.expose
class NodoSubasta:
    def __init__(self, articulo: str, es_primario: bool = True, host: str | None = None, puerto: int | None = None):
        self._articulo = articulo
        self._ultimo_evento = time.time()  # se resetea con cada oferta aceptada
        self._mejor_oferta = 0.0
        self._mejor_postor: str | None = None
        self._cerrada = False
        self._iniciada = False
        self._reloj = RelojLamport()
        self._lock = threading.Lock()
        self._seq_op= 0
        # Recibido por parametro en vez de hardcodeado: todavia no hay
        # eleccion, asi que el rol se fija "a mano" al arrancar el proceso
        # (ver --backup en main). Cuando se implemente la eleccion, este
        # valor va a depender de su resultado en vez de fijarse al inicio.
        self._es_primario = es_primario

        # Proxies a los demas nodos del cluster, para poder replicarles el
        # estado. Solo el primario los necesita (un backup no le replica a
        # nadie hasta que gane una eleccion y pase a ser primario).
        self._backups = []
        if self._es_primario and host is not None and puerto is not None:
            for h, p in config.otros_nodos(host, puerto):
                proxy = Pyro5.api.Proxy(config.uri_de(h, p))
                proxy._pyroTimeout = TIMEOUT_REPLICACION_SEG
                self._backups.append(proxy)

        #esto permite informar a los clientes cuando la subasta se cierra, el server les pushea la informacion
        self._clientes = []
        # el backup no corre su propio reloj de cierre: su estado (incluida
        # la cuenta regresiva) le llega replicado desde el primario.
        if self._es_primario:
            threading.Thread(target=self._vigila_cierre, daemon=True).start() # thread aparte ya que el requestLoop de Pyro es bloqueante y se queda atendiendo clientes
        self._ganador_anunciado = False #flag para que solo se notifique una vez el cierre de la subasta, si se hacen varias rondas se deberia resetear esta flag

    def iniciar_subasta(self) -> bool:
        """El operador del servidor llama a esto presionando 's' para arrancar la subasta."""
        with self._lock:
            if not self._es_primario:
                print("[nodo] Este nodo es backup, no puede iniciar la subasta.")
                return False
            if self._iniciada:
                print("[nodo] La subasta ya estaba iniciada.")
                return False
            self._iniciada = True
            self._ultimo_evento = time.time()
            print(f"[nodo] ¡SUBASTA INICIADA! Articulo: {self._articulo!r}, ventana de {DURACION_VENTANA_SEG}s activa.")
            self._notificar_clientes()
            self._replicar_a_backups(self._estado_actual())
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
            if not self._es_primario:
                return {
                    "aceptada": False,
                    "motivo": "Este nodo es backup, no acepta ofertas",
                    "estado": self._estado_actual(),
                }

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

            self._notificar_clientes() #avisar a los clientes que la subasta se cerro

            # Replicacion sincronica: el primario le manda el estado nuevo a
            # cada backup ANTES de responderle al cliente. Si algun backup no
            # contesta a tiempo, se lo salteamos (ver _replicar_a_backups) en
            # vez de trabar toda la subasta esperandolo.
            self._replicar_a_backups(self._estado_actual())

            return {
                "aceptada": True,
                "motivo": "Oferta aceptada",
                "estado": self._estado_actual(),
            }

    def obtener_estado(self) -> dict:
        with self._lock:
            return self._estado_actual()

    def replicar_estado(self, estado: dict):
        """
        El primario llama esto en cada backup despues de aceptar una oferta,
        de iniciar la subasta o de cerrarla. El backup simplemente pisa su
        copia local con lo que le llega (no recalcula nada: confia en el
        primario, que es quien tiene la fuente de verdad).
        """
        with self._lock:
            if estado["seq_op"] not in (self._seq_op, self._seq_op + 1):
                # se salteo uno o mas numeros de operacion: este backup se
                # perdio una actualizacion (por ej. porque justo no
                # respondio a tiempo esa vez). No hay recuperacion automatica
                # todavia (quedaria para un "resync" completo a futuro), pero
                # al menos lo dejamos loggeado y el backup se pone al dia con
                # este ultimo estado igual.
                print(f"[nodo][backup] ALERTA: salto de seq_op ({self._seq_op} -> {estado['seq_op']}), se perdio una actualizacion")

            self._articulo = estado["articulo"]
            self._mejor_oferta = estado["mejor_oferta"]
            self._mejor_postor = estado["mejor_postor"]
            self._iniciada = estado["iniciada"]
            self._cerrada = estado["cerrada"]
            self._seq_op = estado["seq_op"]
            self._reloj.actualizar(estado["clock_lamport"])
            # aproximamos "cuando arranco la ventana" a partir del tiempo
            # restante recibido, para que si este backup pasa a ser primario
            # la cuenta regresiva siga de donde iba.
            if self._iniciada:
                self._ultimo_evento = time.time() - (DURACION_VENTANA_SEG - estado["tiempo_restante_seg"])

            print(f"[nodo][backup] estado replicado: mejor_oferta={self._mejor_oferta} mejor_postor={self._mejor_postor!r} seq_op={self._seq_op}")

    def _replicar_a_backups(self, estado: dict):
        for proxy in self._backups:
            try:
                proxy._pyroClaimOwnership()  # este proxy tambien lo puede tocar el hilo de _vigila_cierre
                proxy.replicar_estado(estado)
            except Pyro5.errors.CommunicationError:
                # no lo sacamos de self._backups: es la lista fija de config.py,
                # puede haberse caido momentaneamente y responder de nuevo la
                # proxima vez.
                print(f"[nodo] backup {proxy._pyroUri} no respondio, se sigue sin el (seq_op={estado['seq_op']})")

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
                    self._replicar_a_backups(self._estado_actual())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--articulo", default="Articulo de prueba")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--puerto", type=int, required=True)
    parser.add_argument(
        "--backup",
        action="store_true",
        help="arranca este nodo como backup en vez de primario (provisorio, hasta que exista la eleccion)",
    )
    args = parser.parse_args()

    nodo = NodoSubasta(args.articulo, es_primario=not args.backup, host=args.host, puerto=args.puerto)

    daemon = Pyro5.api.Daemon(host=args.host, port=args.puerto)
    daemon.register(nodo, objectId=config.OBJECT_ID)

    rol = "PRIMARIO" if nodo.es_primario() else "BACKUP"
    print(f"Nodo escuchando en {args.host}:{args.puerto} como {rol}")
    print(f"Articulo: {args.articulo!r}, ventana: {DURACION_VENTANA_SEG}s")
    print(f"URI: {config.uri_de(args.host, args.puerto)}")

    # Cambié daemon.requestLoop() por un hilo,  esto es para que el hilo principal no espere a que terminen todas las llamadas de los clientes y pueda ejecutar el comando 's' para iniciar la subasta
    hilo_daemon = threading.Thread(target=daemon.requestLoop, daemon=True)
    hilo_daemon.start()

    if not nodo.es_primario():
        # un backup no inicia nada por su cuenta: solo escucha y espera que
        # el primario le vaya replicando el estado via replicar_estado().
        print(">>> Nodo backup activo, esperando replicacion del primario (Ctrl+C para salir) <<<")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nServidor cerrado.")
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
        print("\nServidor cerrado.")


if __name__ == "__main__":
    main()
