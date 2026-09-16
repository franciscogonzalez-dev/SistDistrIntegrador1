"""
Lista estatica de nodos que pueden formar parte del cluster (ya sea como
primario o como backup). No usamos name server: todos -clientes y nodos-
conocen esta lista de antemano, y cada nodo escucha en una IP:puerto fija.

En este punto (todavia sin eleccion) el primero de la lista actua siempre
como primario. Cuando se implemente la eleccion, cualquiera de estos nodos
podria terminar siendo el primario, por eso el cliente los recorre a todos
en vez de asumir que siempre es el primero.
"""


NODOS = [
    ("172.16.216.121", 9091),
    ("172.16.56.17", 9092),
    ("172.16.86.122", 9093),
]

# Nombre de objeto Pyro fijo e igual en todos los nodos. Al ser siempre el
# mismo, se puede construir el URI directamente sin consultar a nadie.
OBJECT_ID = "nodo_subasta"


def uri_de(host: str, puerto: int) -> str:
    return f"PYRO:{OBJECT_ID}@{host}:{puerto}"


def otros_nodos(host: str, puerto: int):
    """Todos los nodos de NODOS excepto el propio (host, puerto). El primario
    usa esto para saber a quien replicarle el estado."""
    return [(h, p) for (h, p) in NODOS if (h, p) != (host, puerto)]


