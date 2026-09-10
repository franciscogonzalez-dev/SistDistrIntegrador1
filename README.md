# Sistema de subastas distribuido — camino feliz
 falta los backups y la tolerancia a fallos

## como levantarlo

**1) name server** (permite que el cliente encuentre al primario por nombre):
```
python -m Pyro5.nameserver
```

**2) primario:**
```
run python -m nodoPrimario.servidor --articulo "Cuadro" --duracion 120
```
`--duracion` son los segundos que dura la subasta en total.

**3) cliente(s)** — se pueden abrir varios en paralelo:
```
python -m cliente.cliente --id ana
python -m cliente.cliente --id beto
```

Cada cliente te va a pedir un monto para ofertar. Si es mayor a la oferta
actual, se acepta; si no, se rechaza con el motivo.

## estructura del proyecto


comun/
  reloj_lamport.py   -> reloj logico de Lamport
  protocolo.py        -> formato de los mensajes (Oferta, EstadoSubasta)
primario/
  servidor.py          -> nodo primario, expone metodos via Pyro5
cliente/
  cliente.py            -> cliente de consola


## to do list

- Agregar backups que repliquen el estado (requisito 3).
- Deteccion de falla + algoritmo de eleccion (requisito 4).
- Reconexion automatica del cliente si el primario cae (requisito 1).
