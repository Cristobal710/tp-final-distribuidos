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
Los flujos de procesamiento de cada consulta se describen en la sección siguiente.

# Requisitos Funcionales

El cliente establece una conexion con el servicio, declara que tipo de consulta debe procesar como parte del handshake y empieza a leer el archivo en memoria. Debera agrupar las filas crudas en batches. 
La lectura del archivo y la formación de los batches de entrada son responsabilidad del cliente.

Según la query establecida a consultar, el gateway redirije los mensajes del cliente a la queue de procesamiento correcta.

Se detectan las siguientes entidades de computo independientes entre si:

## Filter 
Será el grupo de nodos encargados de: 
* Recibir batches de rows crudas.
* Aplicar el filtro (requerido por la consulta) sobre las filas recibidas.
* Eliminar las columnas innecesarias.
* Direccionar el resultante a la queue correcta.

## Text Processor
Será el grupo de nodos encargados de:
* Recibir batches de rows filtradas.
* Filtrar Stop Words.
* Construir Set Parciales de palabras unicas.
* Construir Sumas Parciales de palabras totales.

## Aggregator
Será el grupo de nodos encargados de:
* Unificar Set Parciales
* Unificar Sumas Parciales

## Joinner
Será el grupo de nodos encargados de:
* Unificar los resultados los procesamientos para entregar al Gateway el resultado final
* Buscar las fotos en caso necesario

# Flujos Esperados
## 1. 
### Url y fecha de modificación para artículos escritos en inglés hasta 2020 inclusive.

```mermaid
flowchart LR
    C1(["Cliente A"]) --> QIN
    C2(["Cliente B"]) --> QIN
    QIN[["Cola de entrada<br/>"]] --> GWS_IN

    subgraph GWS_IN["Gateways stateless (N instancias)"]
        G1["Gateway 1"]
        G2["Gateway 2"]
        GN["Gateway N"]
    end

    GWS_IN --> QF[["Cola FIFO<br/>"]]

    subgraph FILTERS["Filters (N instancias)"]
        F1["Filter 1"]
        F2["Filter 2"]
        FN["Filter N"]
    end

    QF --> F1
    QF --> F2
    QF --> FN

    F1 --> QR
    F2 --> QR
    FN --> QR

    QR[["Cola de resultados<br/>"]] --> Gateways

```

## 2. 
### Nombre, url e imágen (bytes) de artículos escritos hasta 2020 inclusive que incluyan “biography” o “biographie” entre sus secciones.

```mermaid
flowchart LR
    C1(["Cliente A"]) --> QIN
    C2(["Cliente B"]) --> QIN
    QIN[["Cola de entrada<br/>"]] --> GWS_IN

    subgraph GWS_IN["Gateways stateless (N instancias)"]
        G1["Gateway 1"]
        G2["Gateway 2"]
        GN["Gateway N"]
    end

    GWS_IN --> QF[["Cola FIFO<br/>"]]

    subgraph FILTERS["Filters (N instancias)"]
        F1["Filter 1"]
        F2["Filter 2"]
        FN["Filter N"]
    end

    QF --> F1
    QF --> F2
    QF --> FN

    F1 --> QJ
    F2 --> QJ
    FN --> QJ

    QJ[["Cola FIFO<br/>"]]

    subgraph JOINNERS["Joinners (N instancias)"]
        J1["Joinner 1"]
        J2["Joinner 2"]
        JN["Joinner N"]
    end

    QJ --> J1
    QJ --> J2
    QJ --> JN

    J1 --> QR
    J2 --> QR
    JN --> QR

    QR[["Cola de resultados<br/>"]] --> Gateways
```