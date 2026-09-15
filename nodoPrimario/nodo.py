"""
Nodo del sistema de subastas distribuido expuesto mediante Pyro5.
Orquesta los componentes de Subasta, Clientes, Replicacion y Eleccion.
"""

import threading
import time
import Pyro5.api

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
        host: str = "localhost",
        puerto: int | None = None,
    ):
        self._host = host
        self._puerto = puerto
        self._identificador = f"{puerto}" if puerto else "nodo"

        if ronda_autos is None:
            ronda_autos = [{"marca": "Auto", "modelo": "Prueba", "anio": 2000, "kilometraje": 0, "fallas_defectos": "", "imagenes": []}]
        self.subasta = GestorSubasta(ronda_autos, duracion_ventana=DURACION_VENTANA_SEG)
        self.clientes = GestorClientes(identificador=self._identificador)
        self.replicador = GestorReplicacion(host, puerto) if (host and puerto) else None
        self.eleccion = MonitorEleccion(
            host=host,
            puerto=puerto,
            es_primario=es_primario,
            on_promocion=self._al_ser_promovido,
        )

        if es_primario:
            if self.replicador:
                self.replicador.conectar_backups()
            self._iniciar_vigilancia_cierre()
        else:
            self.eleccion.iniciar_vigilancia()

    def _iniciar_vigilancia_cierre(self):
        """Inicia el hilo para monitorear el timeout de la subasta."""
        threading.Thread(target=self._vigila_cierre, daemon=True).start()

    def _al_ser_promovido(self):
        """Callback invocado por MonitorEleccion cuando este nodo gana la eleccion."""
        if not self.replicador:
            self.replicador = GestorReplicacion(self._host, self._puerto)
        self.replicador.conectar_backups()
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

    def obtener_estado(self) -> dict:
        ubicacion = self.ubicacion_primario()
        return self.subasta.estado_actual(ubicacion["host"], ubicacion["puerto"])

    def ofertar(self, cliente_id: str, incremento: float, clock_cliente: int, seq_op_cliente: int) -> dict:
        """
        Un cliente llama a esto para ofertar un incremento.
        """
        time.sleep(0.5)  # simula concurrencia de ofertas
        if not self.es_primario():
            return {
                "aceptada": False,
                "motivo": "Este nodo es backup, no acepta ofertas",
                "estado": self.obtener_estado(),
            }

        ubicacion = self.ubicacion_primario()
        aceptada, motivo, estado = self.subasta.procesar_oferta(
            cliente_id=cliente_id,
            incremento=incremento,
            clock_cliente=clock_cliente,
            seq_op_cliente=seq_op_cliente,
            primario_host=ubicacion["host"],
            primario_puerto=ubicacion["puerto"],
        )

        if aceptada:
            print(f"[{self._puerto}][PRIMARIO][subasta] Nueva mejor oferta: {cliente_id} -> {self.subasta.mejor_oferta}")
            # Notificar push a clientes conectados
            self.clientes.notificar(estado)
            # Replicacion sincronica a los backups
            if self.replicador:
                self.replicador.replicar_a_backups(estado)

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
        self.eleccion.actualizar_primario(estado["primario_host"], estado["primario_puerto"])

    def subscribir_cliente(self, uri_cliente: str):
        """Registra un cliente para enviarle notificaciones de subasta."""
        self.clientes.subscribir(uri_cliente)

    def _vigila_cierre(self):
        """Hilo periodico que revisa si se cumplio la ventana de 30s sin nuevas ofertas."""
        while True:
            time.sleep(1.0)
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

                if self.subasta.siguiente_auto():
                    print(f"[{self._puerto}][PRIMARIO][subasta] INICIANDO SIGUIENTE AUTO.")
                    nuevo_estado = self.obtener_estado()
                    self.clientes.notificar(nuevo_estado)
                    if self.replicador:
                        self.replicador.replicar_a_backups(nuevo_estado)
                else:
                    print(f"[{self._puerto}][PRIMARIO][subasta] RONDA FINALIZADA.")
