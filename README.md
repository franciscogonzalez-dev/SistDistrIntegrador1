
ya no hay name server: cada nodo escucha en una IP:puerto fija, definida en
`comun/config.py` (por defecto, 3 direcciones en localhost). El cliente
conoce esa misma lista de antemano y la recorre preguntando si el nodo es el primario hasta encontrar al nodo correcto.

**1) primario** (por ahora, siempre el primero de la lista):
```
python -m nodoPrimario.servidor --host localhost --puerto 9091 --articulo "aceituna"
```

**2) cliente(s)** — podes abrir varios en paralelo:
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
  servidor.py             -> nodo (por ahora siempre primario), expone metodos via Pyro5
cliente/
  cliente.py                -> cliente de consola con descubrimiento por lista fija
```

## proximos pasos

- agregar backups que repliquen el estado (requisito 3) — usan la misma
  `comun/config.py` para saber donde estan sus pares.
- deteccion de falla + algoritmo de eleccion (requisito 4) — cuando un backup
  gane la eleccion, pone su propio `_es_primario = True`. El cliente ya sabe
  reaccionar: si el nodo que tenia guardado deja de responder, vuelve a
  recorrer la lista y lo encuentra solo.
