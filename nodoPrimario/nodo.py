"""
Nodo del sistema de subastas distribuido expuesto mediante Pyro5.
Orquesta los componentes de Subasta, Clientes, Replicacion y Eleccion.
"""

import threading
import time
import Pyro5.api

from comun import config
from nodoPrimario.subasta import GestorSubasta, DURACION_VENTANA_SEG
from nodoPrimario.clientes import GestorClientes
from backup.replicacion import GestorReplicacion
from backup.eleccion import MonitorEleccion


@Pyro5.api.expose
class NodoSubasta:
    def __init__(
        self,
        ronda_autos: list[dict] = None,
        es_primario: bool = True,
        host: str = "127.0.0.1",
        puerto: int | None = None,
    ):
        self._host = host
        self._puerto = puerto
        self._identificador = f"{puerto}" if puerto else "nodo"
        self._lock_cierre = threading.Lock()
        self._hilo_cierre_activo = False

        if ronda_autos is None:
            ronda_autos = [{"marca": "Auto", "modelo": "Prueba", "anio": 2000, "kilometraje": 0, "fallas_defectos": "", "imagenes": []}]
        self.subasta = GestorSubasta(ronda_autos, duracion_ventana=DURACION_VENTANA_SEG)
        self.clientes = GestorClientes(identificador=self._identificador)
        self.replicador = GestorReplicacion(host, puerto) if (host and puerto) else None

        # Verificar si ya existe otro nodo primario activo en el cluster
        primario_existente = self._detectar_primario_existente()

        if primario_existente:
            h_prim, p_prim, estado_prim = primario_existente
            es_primario = False
            self.subasta.aplicar_estado_replicado(estado_prim)
            if "clientes_uris" in estado_prim and estado_prim["clientes_uris"]:
                self.clientes.sincronizar_uris(estado_prim["clientes_uris"])

            self.eleccion = MonitorEleccion(
                host=host,
                puerto=puerto,
                es_primario=False,
                on_promocion=self._al_ser_promovido,
                obtener_estado_local=self._obtener_estado_local,
            )
            self.eleccion.actualizar_primario(h_prim, p_prim)
            self.eleccion.iniciar_vigilancia()
            print(f"[{self._puerto}][BACKUP] Se detectó primario activo en {h_prim}:{p_prim}. Este nodo se incorpora como RÉPLICA DISPONIBLE.")
        else:
            self.eleccion = MonitorEleccion(
                host=host,
                puerto=puerto,
                es_primario=es_primario,
                on_promocion=self._al_ser_promovido,
                obtener_estado_local=self._obtener_estado_local,
            )
            if es_primario:
                if self.replicador:
                    self.replicador.conectar_backups()
                    self.replicador.replicar_a_backups(self.obtener_estado())
                self._iniciar_vigilancia_cierre()
            else:
                self.eleccion.iniciar_vigilancia()

    def _detectar_primario_existente(self) -> tuple[str, int, dict] | None:
        """Verifica si ya existe un primario activo en el cluster para unirse como replica."""
        for h, p in config.otros_nodos(self._host, self._puerto):
            try:
                proxy = Pyro5.api.Proxy(config.uri_de(h, p))
                proxy._pyroTimeout = 0.8
                if proxy.es_primario():
                    estado = proxy.obtener_estado()
                    return (h, p, estado)
            except Exception:
                continue
        return None

    def _obtener_estado_local(self) -> tuple[int, int]:
        """Devuelve (clock_lamport, seq_op) locales para el algoritmo de eleccion sin RPC."""
        return (self.subasta.reloj.valor(), self.subasta.seq_op)

    def _iniciar_vigilancia_cierre(self):
        """Inicia el hilo para monitorear el timeout de la subasta si no esta ya corriendo."""
        with self._lock_cierre:
            if self._hilo_cierre_activo:
                return
            self._hilo_cierre_activo = True
            threading.Thread(target=self._vigila_cierre, daemon=True).start()

    def _al_ser_promovido(self):
        """Callback invocado por MonitorEleccion cuando este nodo gana la eleccion."""
        if not self.replicador:
            self.replicador = GestorReplicacion(self._host, self._puerto)
        self.replicador.conectar_backups()
        estado = self.obtener_estado()
        self.replicador.replicar_a_backups(estado)
        # Notificar de inmediato a todos los clientes que ya estaban suscritos
        self.clientes.notificar(estado)
        self._iniciar_vigilancia_cierre()

    # --- Metodos expuestos a Pyro5 ---

    def iniciar_subasta(self) -> bool:
        """El operador llama a esto para arrancar la subasta."""
        if not self.es_primario():
            print(f"[{self._puerto}][BACKUP] Este nodo es backup, no puede iniciar la subasta.")
            return False

        exito, msg = self.subasta.iniciar()
        if not exito:
            print(f"[{self._puerto}][PRIMARIO] {msg}")
            return False

        print(f"[{self._puerto}][PRIMARIO] {msg}")
        estado = self.obtener_estado()
        self.clientes.notificar(estado)
        if self.replicador:
            self.replicador.replicar_a_backups(estado)
        return True

    def es_primario(self) -> bool:
        return self.eleccion.es_primario()

    def ping(self) -> bool:
        return True

    def ubicacion_primario(self) -> dict:
        return self.eleccion.ubicacion_primario()

    def obtener_info_eleccion(self) -> dict:
        """Endpoint RPC ligero en memoria para el algoritmo de eleccion Bully."""
        clock_propio, seq_propio = self._obtener_estado_local()
        return {
            "host": self._host,
            "puerto": self._puerto,
            "clock_lamport": clock_propio,
            "seq_op": seq_propio,
            "es_primario": self.es_primario(),
        }

    def obtener_estado(self) -> dict:
        """Retorna el estado actual de la subasta sin bloquear realizando pings."""
        h, p = self.eleccion.primario_actual()
        estado = self.subasta.estado_actual(h, p)
        estado["clientes_uris"] = self.clientes.obtener_uris()
        return estado

    def ofertar(self, cliente_id: str, incremento: float, clock_cliente: int, seq_op_cliente: int) -> dict:
        """
        Un cliente llama a esto para ofertar un incremento.
        """
        time.sleep(0.5)  # simula concurrencia de ofertas
        if not self.es_primario():
            # Si un cliente llamo a este nodo y no es primario, verificar si el primario cayo
            ubicacion = self.eleccion.ubicacion_primario()
            if not self.es_primario():
                return {
                    "aceptada": False,
                    "motivo": "Este nodo es backup, no acepta ofertas",
                    "estado": self.obtener_estado(),
                }
        else:
            ubicacion = {"host": self._host, "puerto": self._puerto}

        aceptada, motivo, estado = self.subasta.procesar_oferta(
            cliente_id=cliente_id,
            incremento=incremento,
            clock_cliente=clock_cliente,
            seq_op_cliente=seq_op_cliente,
            primario_host=ubicacion["host"],
            primario_puerto=ubicacion["puerto"],
        )
        estado["clientes_uris"] = self.clientes.obtener_uris()

        if aceptada:
            print(f"[{self._puerto}][PRIMARIO][subasta] Nueva mejor oferta: {cliente_id} -> {self.subasta.mejor_oferta}")
            # Notificar push a clientes conectados
            self.clientes.notificar(estado)
            # Replicacion semi-sincronica: bloquea hasta el ACK del primer backup
            # (o hasta el timeout si ninguno responde a tiempo) antes de confirmarle
            # la oferta al cliente; el resto de los backups se actualizan en paralelo.
            if self.replicador:
                confirmado = self.replicador.replicar_a_backups(estado)
                if not confirmado:
                    print(
                        f"[{self._puerto}][PRIMARIO][replicacion] ADVERTENCIA: ningun backup confirmo "
                        f"a tiempo (seq_op={estado['seq_op']})"
                    )

        return {
            "aceptada": aceptada,
            "motivo": motivo,
            "estado": estado,
        }

    def replicar_estado(self, estado: dict):
        """
        Llamado por el primario en cada backup tras aceptar oferta, iniciar o cerrar subasta.
        """
        if self.replicador:
            self.replicador.aplicar_estado(self.subasta, estado)
        else:
            self.subasta.aplicar_estado_replicado(estado)
        if "clientes_uris" in estado and estado["clientes_uris"]:
            self.clientes.sincronizar_uris(estado["clientes_uris"])
        self.eleccion.actualizar_primario(estado["primario_host"], estado["primario_puerto"])

    def subscribir_cliente(self, uri_cliente: str):
        """Registra un cliente para enviarle notificaciones de subasta y replica."""
        self.clientes.subscribir(uri_cliente)
        if self.es_primario() and self.replicador:
            self.replicador.replicar_a_backups(self.obtener_estado())

    def desubscribir_cliente(self, uri_cliente: str):
        """Registra la salida de un cliente y remueve su suscripcion push."""
        self.clientes.desubscribir(uri_cliente)
        if self.es_primario() and self.replicador:
            self.replicador.replicar_a_backups(self.obtener_estado())

    def recibir_election(
        self, host_emisor: str, puerto_emisor: int, info_emisor: dict | None = None
    ) -> bool:
        """
        Mensaje ELECTION del algoritmo Bully: otro nodo nos avisa que inicio
        una eleccion. Si tenemos mayor prioridad, respondemos OK y lanzamos
        nuestra propia eleccion.
        """
        return self.eleccion.recibir_election(host_emisor, puerto_emisor, info_emisor)

    def recibir_ok(self, host_emisor: str, puerto_emisor: int):
        """
        Mensaje OK del algoritmo Bully: un nodo superior nos confirma que se
        hara cargo de la eleccion. Cancelamos nuestra candidatura.
        """
        self.eleccion.recibir_ok(host_emisor, puerto_emisor)

    def recibir_coordinator(
        self, host_coordinador: str, puerto_coordinador: int, info_coordinador: dict | None = None
    ):
        """
        Mensaje COORDINATOR del algoritmo Bully: el nuevo primario se anuncia
        a todos los nodos vivos del cluster.
        """
        self.eleccion.recibir_coordinator(host_coordinador, puerto_coordinador, info_coordinador)

    def _vigila_cierre(self):
        """Hilo periodico que revisa si se cumplio la ventana de 30s sin nuevas ofertas."""
        try:
            while self.es_primario():
                time.sleep(1.0)
                if not self.es_primario():
                    break
                if self.subasta.verificar_cierre():
                    print(
                        f"[{self._puerto}][PRIMARIO][subasta] SUBASTA CERRADA. "
                        f"Ganador: {self.subasta.mejor_postor!r} con ${self.subasta.mejor_oferta}"
                    )
                    estado = self.obtener_estado()
                    estado["mensaje_transicion"] = "Preparate para la siguiente subasta..."
                    self.clientes.notificar(estado)
                    if self.replicador:
                        self.replicador.replicar_a_backups(estado)
                    
                    print(f"[{self._puerto}][PRIMARIO][subasta] Esperando 5 segundos para la proxima subasta...")
                    time.sleep(5.0)
                    if not self.es_primario():
                        break

                    if self.subasta.siguiente_auto():
                        print(f"[{self._puerto}][PRIMARIO][subasta] INICIANDO SIGUIENTE AUTO.")
                        nuevo_estado = self.obtener_estado()
                        self.clientes.notificar(nuevo_estado)
                        if self.replicador:
                            self.replicador.replicar_a_backups(nuevo_estado)
                    else:
                        print(f"[{self._puerto}][PRIMARIO][subasta] RONDA FINALIZADA.")
                        estado_final = self.obtener_estado()
                        estado_final["ronda_finalizada"] = True
                        self.clientes.notificar(estado_final)
                        if self.replicador:
                            self.replicador.replicar_a_backups(estado_final)
        finally:
            with self._lock_cierre:
                self._hilo_cierre_activo = False
