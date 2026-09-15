"""
Modulo que encapsula el estado y las reglas de negocio de la subasta.
Independiente de la capa de red / RPC (Pyro5).
"""

import threading
import time

from comun.protocolo import EstadoSubasta
from comun.reloj_lamport import RelojLamport

DURACION_VENTANA_SEG = 30.0


class GestorSubasta:
    def __init__(self, ronda_autos: list[dict], duracion_ventana: float = DURACION_VENTANA_SEG):
        self.ronda_autos = ronda_autos
        self.indice_actual = 0
        self.articulo = ronda_autos[0] if ronda_autos else {}
        self.duracion_ventana = duracion_ventana
        self.ultimo_evento = time.time()
        self.mejor_oferta = 0.0
        self.mejor_postor: str | None = None
        self.cerrada = False
        self.iniciada = False
        self.reloj = RelojLamport()
        self.seq_op = 0
        self._lock = threading.Lock()
        self._ganador_anunciado = False

    def iniciar(self) -> tuple[bool, str]:
        """Inicia la subasta. Retorna (exito, mensaje)."""
        with self._lock:
            if self.iniciada:
                return False, "La subasta ya estaba iniciada."
            self.iniciada = True
            self.ultimo_evento = time.time()
            return True, f"¡SUBASTA INICIADA! Auto: {self.articulo.get('marca')} {self.articulo.get('modelo')}, ventana de {self.duracion_ventana}s activa."

    def siguiente_auto(self) -> bool:
        """Avanza al siguiente auto en la ronda si lo hay. Retorna True si pudo avanzar, False si no hay mas autos."""
        with self._lock:
            if self.indice_actual + 1 < len(self.ronda_autos):
                self.indice_actual += 1
                self.articulo = self.ronda_autos[self.indice_actual]
                self.mejor_oferta = 0.0
                self.mejor_postor = None
                self.cerrada = False
                self.iniciada = True
                self._ganador_anunciado = False
                self.ultimo_evento = time.time()
                self.seq_op += 1
                return True
            return False

    def tiempo_restante(self) -> float:
        """Calcula el tiempo restante de la ventana actual."""
        with self._lock:
            if not self.iniciada:
                return self.duracion_ventana
            restante = self.duracion_ventana - (time.time() - self.ultimo_evento)
            return max(0.0, restante)

    def _esta_cerrada_unlocked(self) -> bool:
        if not self.iniciada:
            return False
        restante = self.duracion_ventana - (time.time() - self.ultimo_evento)
        return self.cerrada or restante <= 0

    def verificar_cierre(self) -> bool:
        """
        Devuelve True una unica vez cuando la subasta pasa de abierta a cerrada por timeout.
        Utilizado por el monitor de cierre.
        """
        with self._lock:
            if self.iniciada and not self._ganador_anunciado:
                restante = self.duracion_ventana - (time.time() - self.ultimo_evento)
                if restante <= 0:
                    self.cerrada = True
                    self._ganador_anunciado = True
                    return True
            return False

    def estado_actual(self, primario_host: str, primario_puerto: int) -> dict:
        """Retorna el diccionario con el estado actual serializado."""
        with self._lock:
            return self._serializar_estado_unlocked(primario_host, primario_puerto)

    def _serializar_estado_unlocked(self, primario_host: str, primario_puerto: int) -> dict:
        if not self.iniciada:
            restante = self.duracion_ventana
        else:
            restante = max(0.0, self.duracion_ventana - (time.time() - self.ultimo_evento))

        cerrada = self.cerrada or (self.iniciada and restante <= 0)

        estado = EstadoSubasta(
            articulo=self.articulo,
            mejor_oferta=self.mejor_oferta,
            mejor_postor=self.mejor_postor,
            tiempo_restante_seg=restante,
            clock_lamport=self.reloj.valor(),
            seq_op=self.seq_op,
            cerrada=cerrada,
            iniciada=self.iniciada,
        ).serializar()
        estado["primario_host"] = primario_host
        estado["primario_puerto"] = primario_puerto
        estado["ronda_autos"] = self.ronda_autos
        estado["indice_actual"] = self.indice_actual
        return estado

    def procesar_oferta(
        self,
        cliente_id: str,
        incremento: float,
        clock_cliente: int,
        seq_op_cliente: int,
        primario_host: str,
        primario_puerto: int,
    ) -> tuple[bool, str, dict]:
        """
        Valida y procesa una oferta entrante.
        Retorna (aceptada, motivo, estado_resultante).
        """
        with self._lock:
            self.reloj.actualizar(clock_cliente)

            if not self.iniciada:
                return (
                    False,
                    "La subasta aun no ha iniciado. Esperando a que el servidor la inicie...",
                    self._serializar_estado_unlocked(primario_host, primario_puerto),
                )

            if self._esta_cerrada_unlocked():
                self.cerrada = True
                return (
                    False,
                    "La subasta ya cerro",
                    self._serializar_estado_unlocked(primario_host, primario_puerto),
                )

            if incremento <= 0:
                return (
                    False,
                    "El incremento debe ser positivo",
                    self._serializar_estado_unlocked(primario_host, primario_puerto),
                )

            # Validar que el cliente no tenga un estado desactualizado
            if seq_op_cliente != self.seq_op:
                return (
                    False,
                    f"El monto cambio a {self.mejor_oferta} antes de procesar tu oferta, volve a intentar",
                    self._serializar_estado_unlocked(primario_host, primario_puerto),
                )

            nuevo_monto = self.mejor_oferta + incremento
            self.mejor_oferta = nuevo_monto
            self.mejor_postor = cliente_id
            self.ultimo_evento = time.time()  # reinicia la ventana de 30s
            self.reloj.tick()
            self.seq_op += 1

            return (
                True,
                "Oferta aceptada",
                self._serializar_estado_unlocked(primario_host, primario_puerto),
            )

    def aplicar_estado_replicado(self, estado: dict):
        """
        Actualiza los datos locales con el estado replicado enviado por el primario.
        Ajusta el reloj de Lamport y el tiempo transcurrido de la ventana.
        """
        with self._lock:
            self.articulo = estado["articulo"]
            self.mejor_oferta = estado["mejor_oferta"]
            self.mejor_postor = estado["mejor_postor"]
            self.iniciada = estado["iniciada"]
            self.cerrada = estado["cerrada"]
            self.seq_op = estado["seq_op"]
            self.reloj.actualizar(estado["clock_lamport"])
            # sin esto, un backup promovido a primario seguiria la ronda con
            # SU PROPIA lista de autos (cargada al azar de autos.json al
            # arrancar), no con la que realmente venia rematando el primario.
            if "ronda_autos" in estado:
                self.ronda_autos = estado["ronda_autos"]
            if "indice_actual" in estado:
                self.indice_actual = estado["indice_actual"]

            # Aproximar cuando arranco la ventana a partir del tiempo restante recibido,
            # para que si este backup pasa a ser primario la cuenta regresiva continue.
            if self.iniciada:
                self.ultimo_evento = time.time() - (self.duracion_ventana - estado["tiempo_restante_seg"])

