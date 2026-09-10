# Sistema de subastas distribuido — camino feliz
 falta los backups y la tolerancia a fallos

## como levantarlo

**1) primario** (reemplazá `192.168.1.10` por la IP de la computadora del servidor):
```
py -m nodoPrimario.servidor --ip 192.168.1.10 --puerto 9002 --articulo "Cuadro" --duracion 120
```
`--duracion` son los segundos que dura la subasta en total. 

**2) cliente(s)** — se pueden abrir varios en paralelo, usando la misma IP y puerto:
```
py -m cliente.cliente --id ana --ip 192.168.1.10 --puerto 9002
py -m cliente.cliente --id beto --ip 192.168.1.10 --puerto 9002
```

El puerto `9002` debe estar permitido en el firewall de la computadora del
servidor. Todos los equipos deben estar en la misma red y tener instalado
Pyro5. El Name Server ya no es necesario para esta conexión directa.

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
