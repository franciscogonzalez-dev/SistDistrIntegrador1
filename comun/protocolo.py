# este seria el protocolo de mensajes entre el/los clientes y el nodo que sea primario
# estos dataclass serian tipo el contrato que todos los nodos van a entender
# para que cada uno no arme su propio formato de msj

from dataclasses import dataclass, asdict
import time


@dataclass
class Oferta: #todo esto es lo q manda un cliente cuando quiere ofertar
    cliente_id: str
    articulo: str
    monto: float
    clock_lamport: int  # el cliente ya incremento su reloj antes de mandar esto

    def serializar(self): # como pyro no sabe serializar objetos python se usa este formato "dict plano"
        return asdict(self)


@dataclass
class EstadoSubasta: # lo que devuelve el nodo primario
    articulo: str
    mejor_oferta: float # en pesos
    mejor_postor: str | None 
    tiempo_restante_seg: float
    clock_lamport: int  # reloj del primario al momento de responder
    cerrada: bool = False # tranquilamente lo podria asumir por el tiempo restante pero tmb sirve
    seq_op: int = 0 #la cantidad de operaciones hasta el momento, se incremente en 1 por cada oferta aceptada,
                    #sirve para detectar si un cliente esta desactualizado del monto actual, y para los backups para ver si estan al dia con el primario

    def serializar(self):
        return asdict(self)


@dataclass
class RespuestaOferta: #confirmacion o rechazo que el primario le manda al ofertante
    aceptada: bool
    motivo: str # esto puede estar o no.
    estado: EstadoSubasta

    def serializar(self):
        return {
            "aceptada": self.aceptada,
            "motivo": self.motivo, #esto en caso de que sea rechazada
            "estado": self.estado.serializar(),
        }


def ahora() -> float:
    return time.time()
