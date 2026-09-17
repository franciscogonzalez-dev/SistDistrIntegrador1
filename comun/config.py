"""
Lista de nodos que forman parte del cluster (primario y backups).
No usamos name server: todos -clientes y nodos- conocen esta lista de antemano,
y cada nodo escucha en una IP:puerto fija.

Cualquiera de estos nodos puede ser electo primario segun el algoritmo de eleccion.
"""

import os

# Soporte para alternar a cluster local mediante la variable LOCAL_CLUSTER=1
if os.environ.get("LOCAL_CLUSTER") == "1":
    NODOS = [
        ("127.0.0.1", 9091),
        ("127.0.0.1", 9092),
        ("127.0.0.1", 9093),
    ]
else:
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
    """Todos los nodos de NODOS excepto el propio (host, puerto)."""
    return [
        (h, p) for (h, p) in NODOS
        if not ((h == host and p == puerto) or (host in ("127.0.0.1", "localhost") and p == puerto and all(hn in ("127.0.0.1", "localhost") for hn, _ in NODOS)))
    ]
