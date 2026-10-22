# Trabajo Práctico Final - Wikipedia

Sistema distribuido que analiza los artículos en inglés y francés del dataset [Wikipedia Structured Contents](https://www.kaggle.com/datasets/wikimedia-foundation/wikipedia-structured-contents) de Kaggle y responde 5 consultas.

## Ejecución

`make up`: inicia los contenedores y sigue los logs de todos en un solo flujo de salida.

`make down`: detiene los contenedores y libera los recursos asociados.

`make logs`: sigue los logs de todos los contenedores.

`make test`: corre los tests dentro de un contenedor.

## Estructura

```
src/
  client/                 cliente
    main.py               configuracion, SIGTERM y arranque
    sender.py             arma los batches y los manda al gateway
    dataset/
      kaggle.py           baja los archivos de Kaggle en un hilo aparte
      reader.py           lee las filas de cada archivo Parquet
    Dockerfile
    requirements.txt
  gateway/                gateway
    main.py               configuracion, SIGTERM y arranque
    server.py             acepta clientes, se queda con las columnas necesarias y publica en clasificacion
    Dockerfile
    requirements.txt
  nodes/                  nodos de procesamiento
    classifier/
      main.py             configuracion, SIGTERM y arranque
      classifier.py       calcula periodo e idioma y reparte en texto, bio y resultados (Q1)
      Dockerfile
      requirements.txt
    text_processor/
      main.py             configuracion, SIGTERM y arranque
      text_processor.py   consume texto, coordina los EOF de los clasificadores con los otros Text processor y da cada texto a las salidas
      words.py            saca las palabras de un abstract (minusculas, sin stopwords ni numeros)
      words_output.py     salida de Q3 y Q4: cuenta palabras por batch y las manda al contador que le toca por hash
      Dockerfile
      requirements.txt
    word_counter/
      main.py
      word_counter.py     guarda en disco las palabras de su shard y, al terminar un cliente, manda las compartidas (Q3) y su top (Q4)
      Dockerfile
      requirements.txt
    join_q3/
      main.py
      join_q3.py          junta las palabras compartidas de todos los contadores y publica Q3
      Dockerfile
      requirements.txt
    top20/
      main.py
      top20.py            junta los tops locales y publica el top 20 de Q4
      Dockerfile
      requirements.txt
  common/                 lo que comparten todos los nodos
    batching/
      batch_builder.py    junta items en batches con tope de cantidad y de bytes
    eof/
      upstream_tracker.py cuenta los EOF de la etapa anterior para saber cuando termino un cliente
    middleware/
      middleware.py       interfaz del middleware
      middleware_rabbitmq.py  implementacion con RabbitMQ
    partition/
      partition.py        hash estable para repartir claves entre instancias
    protocol/
      external.py         mensajes entre cliente y gateway y serializacion
      internal.py         mensajes entre nodos
      articles.py         formato de los batches de articulos
    publisher/
      reconnecting_publisher.py  publicador que abre la conexion al usarla y la renueva si estuvo ociosa
    safe_socket/
      safe_socket.py      lectura y escritura completas sobre sockets
    storage/
      client_files.py     un archivo por cliente para guardar filas hasta que termina
  rabbitmq/
    Dockerfile
tests/                    tests
tools/
  check_q3_q4.py          compara Q3 y Q4 contra un calculo directo, leyendo la cola de resultados
```

## Variables de entorno

### Cliente

| Variable | Descripción | Por defecto |
|---|---|---|
| `SERVER_HOST` | Host del gateway | |
| `SERVER_PORT` | Puerto del gateway | `5678` |
| `KAGGLE_DATASET` | Dataset de Kaggle | `wikimedia-foundation/wikipedia-structured-contents` |
| `DATASET_FILE_PATTERNS` | Archivos a bajar, separados por coma. `{:05}` es el número de archivo | inglés y francés |
| `DATASET_FILES_PER_PATTERN` | Archivos de cada patrón. 0 baja todos | `0` |
| `DATASET_ROWS_PER_PATTERN` | Máximo de filas a mandar de cada patrón (idioma). 0 manda todas | `0` |
| `CLIENT_COLUMNS` | Columnas a mandar, separadas por coma. Vacía manda todas | las 8 que usan las consultas |
| `PARQUET_ROWS_PER_READ` | Filas por bloque al leer cada archivo | `2048` |
| `MAX_BATCH_ROWS` | Máximo de filas por batch | `200` |
| `MAX_BATCH_BYTES` | Máximo de bytes por batch | `1048576` |
| `CONNECT_ATTEMPTS` | Intentos de conexión con el gateway | `10` |
| `CONNECT_RETRY_SECONDS` | Espera entre intentos de conexión | `1` |
| `DOWNLOAD_DIR` | Directorio de las descargas | `/tmp/downloads` |
| `DOWNLOAD_FILES_AHEAD` | Archivos que se bajan por adelantado | `1` |
| `DOWNLOAD_POLL_SECONDS` | Espera entre intentos de dejar un archivo listo | `0.5` |
| `DOWNLOAD_STOP_TIMEOUT_SECONDS` | Espera máxima al hilo de descarga al terminar | `5` |

### Gateway

| Variable | Descripción | Por defecto |
|---|---|---|
| `SERVER_HOST` | Dirección donde escucha | `0.0.0.0` |
| `SERVER_PORT` | Puerto donde escucha | `5678` |
| `GATEWAY_ID` | Identificador del gateway | `0` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `CLASSIFICATION_QUEUE` | Cola donde publica los artículos | `clasificacion` |

### Clasificador

| Variable | Descripción | Por defecto |
|---|---|---|
| `CLASSIFIER_ID` | Identificador del clasificador | `0` |
| `CLASSIFIER_AMOUNT` | Cantidad de clasificadores | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `CLASSIFICATION_QUEUE` | Cola de la que consume | `clasificacion` |
| `CONTROL_EXCHANGE` | Exchange por el que se avisan los EOF entre clasificadores | `control_clasificador` |
| `TEXT_QUEUE` | Cola de los textos (Q3, Q4 y Q5) | `texto` |
| `BIO_QUEUE` | Cola de los artículos hasta 2020 (Q2) | `bio` |
| `RESULTS_QUEUE_PREFIX` | Prefijo de las colas de resultados, a las que se les agrega `.<gateway_id>` | `resultados` |

### Text processor

| Variable | Descripción | Por defecto |
|---|---|---|
| `TEXT_PROCESSOR_ID` | Identificador del Text processor | `0` |
| `TEXT_PROCESSOR_AMOUNT` | Cantidad de Text processor | `1` |
| `CLASSIFIER_AMOUNT` | Cantidad de clasificadores, que es cuantos EOF espera | `1` |
| `WORD_COUNTER_AMOUNT` | Cantidad de contadores de palabras (shards) | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `TEXT_QUEUE` | Cola de la que consume | `texto` |
| `CONTROL_EXCHANGE` | Exchange por el que se reenvian los EOF de los clasificadores entre Text processor | `control_text_processor` |
| `WORDS_QUEUE_PREFIX` | Prefijo de las colas de los contadores, a las que se les agrega `.<shard>` | `palabras` |
| `MAX_BATCH_ROWS` | Máximo de filas por mensaje de palabras | `2000` |
| `MAX_BATCH_BYTES` | Máximo de bytes por mensaje de palabras | `1048576` |
| `PUBLISHER_IDLE_SECONDS` | Segundos sin usar una conexion de salida antes de renovarla | `30` |

### Contador de palabras

| Variable | Descripción | Por defecto |
|---|---|---|
| `WORD_COUNTER_ID` | Identificador del shard | `0` |
| `TEXT_PROCESSOR_AMOUNT` | Cantidad de Text processor, que es cuantos EOF espera | `1` |
| `JOIN_Q3_AMOUNT` | Cantidad de Join Q3 | `1` |
| `TOP20_AMOUNT` | Cantidad de Top 20 final | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `WORDS_QUEUE_PREFIX` | Prefijo de su cola, a la que se le agrega `.<WORD_COUNTER_ID>` | `palabras` |
| `JOIN_Q3_QUEUE_PREFIX` | Prefijo de las colas del Join Q3 | `join_q3` |
| `TOP20_QUEUE_PREFIX` | Prefijo de las colas del Top 20 final | `top20` |
| `DATA_DIR` | Directorio donde guarda un archivo por cliente | `/data` |
| `TOP_SIZE` | Cantidad de palabras de su top local | `20` |
| `MAX_BATCH_ROWS` | Máximo de filas por mensaje | `2000` |
| `MAX_BATCH_BYTES` | Máximo de bytes por mensaje | `1048576` |
| `PUBLISHER_IDLE_SECONDS` | Segundos sin usar una conexion de salida antes de renovarla | `30` |

### Join Q3

| Variable | Descripción | Por defecto |
|---|---|---|
| `JOIN_Q3_ID` | Identificador del Join Q3 | `0` |
| `WORD_COUNTER_AMOUNT` | Cantidad de contadores, que es cuantos EOF espera | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `JOIN_Q3_QUEUE_PREFIX` | Prefijo de su cola, a la que se le agrega `.<JOIN_Q3_ID>` | `join_q3` |
| `RESULTS_QUEUE_PREFIX` | Prefijo de las colas de resultados | `resultados` |
| `DATA_DIR` | Directorio donde guarda un archivo por cliente | `/data` |
| `MAX_BATCH_ROWS` | Máximo de filas por mensaje de resultados | `2000` |
| `MAX_BATCH_BYTES` | Máximo de bytes por mensaje de resultados | `1048576` |
| `PUBLISHER_IDLE_SECONDS` | Segundos sin usar una conexion de salida antes de renovarla | `30` |

### Top 20 final

| Variable | Descripción | Por defecto |
|---|---|---|
| `TOP20_ID` | Identificador del Top 20 final | `0` |
| `WORD_COUNTER_AMOUNT` | Cantidad de contadores, que es cuantos EOF espera | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `TOP20_QUEUE_PREFIX` | Prefijo de su cola, a la que se le agrega `.<TOP20_ID>` | `top20` |
| `RESULTS_QUEUE_PREFIX` | Prefijo de las colas de resultados | `resultados` |
| `TOP_SIZE` | Cantidad de palabras del resultado de Q4 | `20` |
| `PUBLISHER_IDLE_SECONDS` | Segundos sin usar una conexion de salida antes de renovarla | `30` |

## Q3 y Q4

Q3 y Q4 comparten el Text processor con Q5. El Text processor saca las palabras de cada abstract una sola vez (igual que el notebook de referencia: minusculas, `\b\w+\b`, sin las stopwords de NLTK en ingles y frances ni numeros) y se las da a la salida de cada consulta. Cada salida implementa:

- `send_batch(client_id, gateway_id, texts) -> {cola: mensajes mandados}`: recibe una lista de `ProcessedText(url, until_2020, language, words)`, donde `words` es la lista de palabras del abstract con repetidas, y publica lo que necesita su consulta.
- `send_eofs(sent, encode)`: manda `encode(cantidad, query=None)` a cada una de sus colas, con la cantidad de mensajes que se le mando a cada una.
- `close()`.

La salida de Q3 y Q4 es `WordsOutput`. La de Q5 se agrega a la lista de salidas que se le pasa a `TextProcessor` en `text_processor/main.py`. Los tipos de mensaje 7 a 9 de `internal.py` son de Q3 y Q4.

Cada palabra va al contador `shard_of(palabra, WORD_COUNTER_AMOUNT)`. El contador guarda en disco lo que le llega de cada cliente y, cuando terminan los datos, manda las palabras que aparecen en artículos en inglés y en francés al Join Q3 y sus `TOP_SIZE` palabras con más artículos al Top 20 final, ambos elegidos por `shard_of(client_id, cantidad)`. El Join Q3 publica las palabras en `resultados.<gateway_id>` como Q3, y el Top 20 final publica las 20 primeras, con empate por orden alfabetico, como Q4.

### EOF

Cada EOF trae `processed`, `total` y `sent`, y despues de mandarlo un nodo avisa con `LATE_EOF` lo que procese de mas. `common/eof/UpstreamTracker` los junta:

- El Text processor comparte una cola con los demas, asi que reenvia cada EOF de los clasificadores a todos por el exchange de control. Cuando tiene el de todos los clasificadores y lo procesado por ellos es igual al total, sabe cuantos textos hubo y manda su EOF a todos los contadores.
- Los contadores, el Join Q3 y el Top 20 final tienen una cola propia por instancia. Terminan a un cliente cuando llegaron los EOF de todos los nodos de la etapa anterior, la suma de lo procesado es igual al total y recibieron todos los mensajes que esos nodos dicen haber mandado.

