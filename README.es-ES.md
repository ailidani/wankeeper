

# WanKeeper

## Resumen rápido

WanKeeper es un servicio de coordinación distribuida para lograr escalabilidad en una WAN. WanKeeper utiliza una arquitectura de agente de tokens jerárquica para tareas de coordinación descentralizada en múltiples centros de datos, y es totalmente compatible con la API de ZooKeeper.

## Configuración

El archivo de configuración es un único archivo de propiedades que incluye los parámetros tanto de ZooKeeper como de WanKeeper. Ejemplo de configuración en `conf/wankeeper.properties`.


|      Nombre     | Valor predeterminado |                 Descripción                |
|:--------------:|:-------------:|:------------------------------------------:|
|       cid      |       1       | id del clúster                             |
|     master     |     false     | es el DC maestro                           |
| master.address |   127.0.0.1   | IPs del DC maestro                         |
|   master.port  |      1735     | puertos del DC maestro                     |
|    leasetype   |       2       | n accesos consecutivos, n=0 es porcentaje  |
|     rwtoken    |      true     | uso de token de lectura/escritura          |



## Primeros pasos

Para iniciar el servidor WanKeeper, ejecute los scripts en la carpeta `bin`.
