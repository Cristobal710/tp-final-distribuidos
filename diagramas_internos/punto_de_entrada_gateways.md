# Diagrama: Punto de entrada y salida del sistema

Cada cliente lee su archivo local y forma batches de filas crudas antes de enviarlos a la cola de entrada. Los gateways reciben esos batches y los publican hacia la cola de preparación. El procesamiento de cada consulta comienza en el pool de preparación y filtros, como se muestra en [Flujos de los requisitos funcionales](requisitos_funcionales.md).

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
