
ya no hay name server: cada nodo escucha en una IP:puerto fija, definida en
`comun/config.py` (por defecto, 3 direcciones en localhost). Los nodos hacen
la eleccion del primario; el cliente solo consulta la ubicacion que le indica
un nodo vivo.

**1) primario inicial** (el nodo que arranca sin `--backup`):
```
python -m nodoPrimario.servidor --host localhost --puerto 9091 --articulo "aceituna"
```
Cuando arranca, el nodo espera que apretes `s` + Enter para iniciar la subasta.

**2) backups** (opcional, para probar la replicacion — usan los puertos definidos en `comun/config.py`):
```
python -m backup.servidor --puerto 9092
python -m backup.servidor --puerto 9093
```
*(Tambien podes seguir usando `python -m nodoPrimario.servidor --puerto 9092 --backup` por compatibilidad).*

Un backup no pide `s`: se queda escuchando y el primario le va empujando el
estado (`replicar_estado`) cada vez que acepta una oferta, inicia o cierra
la subasta. Si el primario se cae, los backups detectan la falta de heartbeat,
consultan cuales siguen vivos y promueven al nodo de mayor prioridad.

**3) cliente(s)** — podes abrir varios en paralelo:
```
python -m cliente.cliente --id alfredo
python -m cliente.cliente --id pablo rosales
```
(O usar la interfaz grafica: `python -m cliente.cliente_gui`)

cada cliente te va a pedir un **incremento** (no un monto absoluto). El
monto final siempre lo calcula el nodo: `mejor_oferta_actual + incremento`.
La subasta tiene una ventana de 30 segundos que se reinicia con cada oferta
aceptada; si nadie oferta en 30s, cierra y gana el ultimo postor.

## estructura

```
comun/
  config.py            -> lista fija de nodos del cluster + construccion de URIs
  reloj_lamport.py     -> reloj logico de Lamport (requisito 5)
  protocolo.py         -> formato de los mensajes (Oferta, EstadoSubasta)
nodoPrimario/
  subasta.py           -> logica pura del negocio (ofertas, tiempos, reloj, seq_op)
  clientes.py          -> suscripcion de clientes y notificaciones push
  nodo.py              -> fachada orquestadora expuesta via Pyro5
  servidor.py          -> entrypoint de consola para el nodo primario
backup/
  replicacion.py       -> sincronizacion y verificacion de estado primario-backup
  eleccion.py          -> heartbeat de deteccion de fallas y algoritmo de eleccion
  servidor.py          -> entrypoint de consola para los nodos backup
cliente/
  cliente.py           -> cliente de consola que sigue la ubicacion del primario
  cliente_gui.py       -> cliente con interfaz grafica (Tkinter)
```

## proximos pasos

- ~~agregar backups que repliquen el estado (requisito 3)~~ — hecho: el
  primario le replica el estado a cada backup (`replicar_estado`) despues de
  cada oferta aceptada, de iniciar o de cerrar la subasta. Es replicacion
  **sincronica**: el primario llama a los backups antes de responderle al
  cliente, pero con un timeout corto (`TIMEOUT_REPLICACION_SEG` en
  `servidor.py`) para no colgarse si alguno esta caido — en ese caso esa
  actualizacion puntual se pierde para ese backup (se loggea la alerta de
  salto de `seq_op`, todavia no hay resync automatico).
- deteccion de falla + algoritmo de eleccion (requisito 4) — hecho: los
  backups detectan la falta de heartbeat y eligen al nodo vivo de mayor
  prioridad. El cliente sigue la ubicacion informada por el cluster y vuelve
  a suscribirse al nuevo primario cuando el anterior deja de responder.
