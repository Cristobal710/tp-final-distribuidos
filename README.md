# tp-final-distribuidos

## Diseño inicial

En una idea inicial, podemos presentar 2 casos de entrada posibles: 

1- Un único gateway de entrada al sistema a través del cual todos los clientes realizan sus pedidos, este gateway distibuye cargas a partir de este gateway en diferentes nodos que comenzaran a realizar diferentes pedidos. 

2- Varios gateways, cada uno conectado a diferentes nodos luego, para que no se mezclen informaciones correspondientes a diferentes clientes. Esto podría solucionarse (igual a trabajos previos) con un id asociado a cada cliente, y compartir instancias de diferentes actividades. 

### Caso 1 de entrada al sistema

#### Ventajas

Con un único gateway vamos a tener simplicidad a la hora de manejar a los clientes. Toda la información del sistema ingresa por un único punto de entrada, por lo que es fácil de comprender y seguir el flujo del request de un cliente en su comienzo. 

A su vez, no hay que coordinar entre gateways o instancias porque toda la información volvería al mismo gateway para enviar al cliente el resultado de su request.

#### Desventajas

Principalmente, la simplicidad nos trae como problema un Unico Punto de Falla (SPOF). Esto no nos generaría un sistema un poco frágil en busqueda de ganar simplicidad, y lo haría poco escalable a su vez. 

### Caso 2 de entrada al sistema 

#### Ventajas

Un sistema que escala mejor ante mayor cantidad de requests de clientes, la distribución de cargas puede soportarse mejor, y ante fallas en un gateway, otro puede tomar temporalmente los requests del caído hasta que vuelva (a debatir).

#### Desventajas

El sistema va a tener mayor complejidad en el manejo de información. 

#### Posible solución

Podemos intentar manejar unicamente información en este gateway (de entrada y de salida) y entonces no tendríamos problemas a la hora de saber a quien envíar información o si esta se encuentra completa o no. El gateway recibe un mensaje a enviar a un cliente con información y simplemente la envía. Al ser stateless, podemos escalar con mayor cantidad de instancias de esta entidad Gateway de ser necesario sin presentarnos problemas.

### Decisión final

Vamos a usar gateways stateless que puedan recibir información de cualquier cliente y enviar información a cualquier cliente. La idea de no tener estado es que sean lo más escalables posibles. Si no tenemos un estado, podemos escalar de la forma más simple posible. 
La idea de escalar es no perder disponibilidad en ningún momento. En contraposición, vamos a tener una complejidad mayor en el procesamiento de los datos, porque pueden llegar de cualquier gateway y no hay una información compartida entre ellos y la parte del sistema que se encarga de procesar la información.

# Diagrama: Punto de entrada y salida del sistema

## ida: recibir request de un cliente

```mermaid
flowchart LR
    C1(["Cliente A"]) --> QIN
    C2(["Cliente B"]) --> QIN
    QIN[["Cola de entrada<br/>compartida"]] --> GWS

    subgraph GWS["Gateways stateless (N instancias)"]
        G1["Gateway 1"]
        G2["Gateway 2"]
        GN["Gateway N"]
    end

    GWS --> PROC{{"Procesamiento de datos"}}

    classDef pendiente stroke-dasharray: 5 5
    class PROC pendiente
```

## vuelta: enviar resultados a medida que van llegando

```mermaid
flowchart RL
    PROC{{"Procesamiento de datos"}} --> RES["Resultados"]
    RES --> GWS

    subgraph GWS["Gateways stateless (N instancias)"]
        G1["Gateway 1"]
        G2["Gateway 2"]
        GN["Gateway N"]
    end

    GWS --> C1(["Cliente A"])
    GWS --> C2(["Cliente B"])

    classDef pendiente stroke-dasharray: 5 5
    class PROC pendiente
```

Aca se muestra una primera idea de como se vería el sistema de entrada del sistema. Si bien se muestran 2 clientes A y B, esto obviamente escalaría a N clientes que deseen utilizar el sistema.
Al no tener una decisión tomada con respecto a como se verá el sistema con respecto a procesar esta información, de momento no se lo diagramó, esto es lo próximo a realizar.
