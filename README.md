
ya no hay name server: cada nodo escucha en una IP:puerto fija, definida en
`comun/config.py` (por defecto, 3 direcciones en localhost). Los nodos hacen
la eleccion del primario; el cliente solo consulta la ubicacion que le indica
un nodo vivo.

**1) primario inicial** (el nodo que arranca sin `--backup`):
```
python -m nodoPrimario.servidor --host localhost --puerto 9091 --articulo "aceituna"
```
Cuando arranca, el nodo espera que apretes `s` + Enter para iniciar la subasta.

**2) backups** (opcional, para probar la replicacion — mismo `--articulo`
para que arranquen todos del mismo lado, y usan los puertos que ya estan en
`comun/config.py`):
```
python -m nodoPrimario.servidor --host localhost --puerto 9092 --backup
python -m nodoPrimario.servidor --host localhost --puerto 9093 --backup
```
Un backup no pide `s`: se queda escuchando y el primario le va empujando el
estado (`replicar_estado`) cada vez que acepta una oferta, inicia o cierra
la subasta. Si el primario se cae, los backups detectan la falta de heartbeat,
consultan cuales siguen vivos y promueven al nodo de mayor prioridad.

**3) cliente(s)** — podes abrir varios en paralelo:
```
python -m cliente.cliente --id alfredo
python -m cliente.cliente --id pablo rosales
```

cada cliente te va a pedir un **incremento** (no un monto absoluto). El
monto final siempre lo calcula el nodo: `mejor_oferta_actual + incremento`.
La subasta tiene una ventana de 30 segundos que se reinicia con cada oferta
aceptada; si nadie oferta en 30s, cierra y gana el ultimo postor.

## estructura

```
comun/
  config.py            -> lista fija de nodos del cluster + construccion de URIs
  reloj_lamport.py      -> reloj logico de Lamport (requisito 5)
  protocolo.py           -> formato de los mensajes (Oferta, EstadoSubasta)
nodoPrimario/
  servidor.py             -> nodo que puede ser primario o backup, expone metodos via Pyro5
cliente/
  cliente.py                -> cliente de consola que sigue la ubicacion del primario
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
