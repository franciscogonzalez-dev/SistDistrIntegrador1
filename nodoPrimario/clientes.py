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
        self._uris: set[str] = set()
        self._lock = threading.Lock()

    def subscribir(self, uri_cliente: str):
        # Registra un cliente para recibir notificaciones asincronas.
        with self._lock:
            if uri_cliente in self._uris:
                return
            self._uris.add(uri_cliente)
            try:
                proxy = Pyro5.api.Proxy(uri_cliente)
                proxy._pyroOneway.add("notificar_estado")
                self._clientes.append(proxy)
            except Exception:
                pass
        print(f"[{self.identificador}][clientes] Un cliente se unio a la sala: {uri_cliente}")

    def sincronizar_uris(self, uris: list[str]):
        """Sincroniza los clientes a partir de la lista de URIs replicada."""
        if not uris:
            return
        with self._lock:
            for uri in uris:
                if uri not in self._uris:
                    self._uris.add(uri)
                    try:
                        proxy = Pyro5.api.Proxy(uri)
                        proxy._pyroOneway.add("notificar_estado")
                        self._clientes.append(proxy)
                    except Exception:
                        pass

    def obtener_uris(self) -> list[str]:
        """Devuelve la lista actual de URIs de clientes suscritos."""
        with self._lock:
            return list(self._uris)

    def notificar(self, estado: dict):
        # Envia el estado actual a todos los clientes registrados.
        with self._lock:
            clientes_actuales = list(self._clientes)

        vivos = []
        uris_vivas = set()
        for proxy in clientes_actuales:
            try:
                proxy._pyroClaimOwnership()
                proxy.notificar_estado(estado)
                vivos.append(proxy)
                uris_vivas.add(str(proxy._pyroUri))
            except Pyro5.errors.CommunicationError:
                pass # El cliente se desconecto o cayo

        with self._lock:
            self._clientes = vivos
            self._uris = uris_vivas

    def limpiar(self): # Limpia la lista de clientes.
        with self._lock:
            self._clientes.clear()
            self._uris.clear()
