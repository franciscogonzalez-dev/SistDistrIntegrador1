"""
Reloj logico de Lamport.

Regla de Lamport:
- Antes de un evento local (o de enviar un mensaje), el proceso incrementa su
  propio contador.
- Al recibir un mensaje con un contador "recibido", el proceso actualiza el
  suyo a max(propio, recibido) + 1.

Esto no da la hora real, da un ORDEN CAUSAL: si el evento A pudo haber influido
en el evento B, entonces clock(A) < clock(B). Es lo que necesitamos para poder
decidir, entre dos ofertas que llegan casi juntas a distintos nodos, cual fue
"antes" sin confiar en el reloj de cada maquina (que puede estar desincronizado).
"""

import threading


class RelojLamport:
    def __init__(self, valor_inicial: int = 0):
        self._valor = valor_inicial
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Incrementa el reloj por un evento local o antes de enviar un mensaje."""
        with self._lock:
            self._valor += 1
            return self._valor

    def actualizar(self, clock_recibido: int) -> int:
        """Sincroniza el reloj al recibir un mensaje con timestamp ajeno."""
        with self._lock:
            self._valor = max(self._valor, clock_recibido) + 1
            return self._valor

    def valor(self) -> int:
        with self._lock:
            return self._valor
