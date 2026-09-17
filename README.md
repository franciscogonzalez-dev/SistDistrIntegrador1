

## Arquitectura y Funcionamiento
- Los nodos monitorizan al primario con heartbeats periódicos. Si el primario cae, ejecutan una elección concurrente eligiendo al nodo con mayor secuencia de operación (`seq_op`), mayor reloj de `Lamport` y desempate por índice.
- Si cae el primario original (9091) y luego el nodo promovido (9092), el último nodo restante (9093) asume la subasta en solitario sin colgarse.
- Si un nodo previamente caído (por ejemplo, el 9091) se vuelve a levantar, detecta automáticamente la existencia de un primario activo en el cluster, sincroniza su estado y se incorpora de inmediato como **RÉPLICA DISPONIBLE** (sin interrumpir la subasta ni pedir confirmación).
- El cliente detecta caídas en tiempo real, se reconecta al nuevo primario con reintentos automáticos, sincroniza su secuencia de operación y reenvía cualquier oferta en vuelo sin que el usuario sufra interrupciones.
- Las listas de clientes suscritos a notificaciones push se replican entre nodos, de modo que el nuevo primario sigue notificando a todos los clientes inmediatamente tras ser promovido.


### 1) Arrancar los Nodos del Cluster

**Nodo 1 (Puerto 9091):**
```bash
python -m nodoPrimario.servidor --host <IP_DEL_CONFIG> --puerto 9091
```
*Si es el primer nodo en arrancar el sistema, esperará que presiones `s` + Enter para iniciar la subasta. Si se levanta cuando ya hay una subasta en curso en otro nodo, se unirá automáticamente como réplica.*

**Nodo 2 (Puerto 9092 - Backup):**
```bash
python -m backup.servidor --host <IP_DEL_CONFIG> --puerto 9092
```

**Nodo 3 (Puerto 9093 - Backup):**
```bash
python -m backup.servidor --host <IP_DEL_CONFIG> --puerto 9093
```

---

### 2) Conectar Clientes

Podes abrir múltiples clientes en paralelo:

**Cliente:**
```bash
python -m cliente.cliente --id ana
python -m cliente.cliente --id carlos
```

---

## 📁 Estructura del Proyecto

```
comun/
  config.py            -> Lista de nodos del cluster (IP/puertos) y construcción de URIs.
  protocolo.py         -> Modelos y dataclasses (Oferta, EstadoSubasta, RespuestaOferta).
  reloj_lamport.py     -> Reloj lógico de Lamport para ordenamiento de eventos.
nodoPrimario/
  subasta.py           -> Lógica del negocio (gestión de ofertas, autos, temporizador de 30s).
  clientes.py          -> Gestión y persistencia de suscripciones de clientes push.
  nodo.py              -> Fachada Pyro5, auto-detección de rol y despacho de operaciones.
  servidor.py          -> Punto de entrada del nodo (primario inicial o réplica viva).
backup/
  eleccion.py          -> Heartbeat de vigilancia y algoritmo de elección concurrente.
  replicacion.py       -> Replicación concurrente y no bloqueante de estado.
  servidor.py          -> Punto de entrada para nodos backup.
cliente/
  cliente.py           -> Cliente con reconexión y reintento automático.
```
