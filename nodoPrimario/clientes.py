"""
Modulo encargado de gestionar las suscripciones de los clientes y
el envio de notificaciones push ante cambios de estado.
"""

import threading
import Pyro5.api
import Pyro5.errors


class GestorClientes:
    def __init__(self, identificador: str = "servidor"):
        self.identificador = identificador
        self._clientes: list[Pyro5.api.Proxy] = []
        self._lock = threading.Lock()

    def subscribir(self, uri_cliente: str):
        #Registra un cliente para recibir notificaciones asincronas.
        proxy = Pyro5.api.Proxy(uri_cliente)
        proxy._pyroOneway.add("notificar_estado")
        with self._lock:
            self._clientes.append(proxy)
        print(f"[{self.identificador}][clientes] Un cliente se unio a la sala: {uri_cliente}")

    def notificar(self, estado: dict):
        #Envia el estado actual a todos los clientes registrados.
        #Reclama la propiedad del proxy (thread safety de Pyro5) y descarta clientes caidos.
        with self._lock:
            clientes_actuales = list(self._clientes)

        vivos = []
        for proxy in clientes_actuales:
            try:
                proxy._pyroClaimOwnership()
                proxy.notificar_estado(estado)
                vivos.append(proxy)
            except Pyro5.errors.CommunicationError:
                pass # El cliente se desconecto o cayo

        with self._lock:
            self._clientes = vivos

    def limpiar(self): #Limpia la lista de clientes.
        with self._lock:
            self._clientes.clear()
