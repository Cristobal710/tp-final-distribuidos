# Trabajo Práctico Final - Wikipedia

Sistema distribuido que analiza los artículos en inglés y francés del dataset [Wikipedia Structured Contents](https://www.kaggle.com/datasets/wikimedia-foundation/wikipedia-structured-contents) de Kaggle y responde 5 consultas.

## Ejecución

`make up`: inicia los contenedores y sigue los logs de todos en un solo flujo de salida.

`make down`: detiene los contenedores y libera los recursos asociados.

`make logs`: sigue los logs de todos los contenedores.

`make test`: corre los tests dentro de un contenedor.

`make test-full`: construye una particion real de 2 GB desde `~/Downloads/distr_dataset.zip`, ejecuta un solo
cliente contra todos los nodos y compara Q1–Q5 recibidas por TCP contra un calculo independiente.
Requiere Docker, Python 3 e Internet para las imagenes reales. Ver [detalle de la suite](tests/full_suite/README.md).

`make run`: borra los resultados anteriores de `client_0`, inicia los contenedores, sigue los logs del cliente y termina cuando el cliente termina (falla si el cliente sale con error).

Cada cliente escribe sus resultados en `results/<cliente>/`, un archivo JSONL por consulta (`q1.jsonl` … `q5.jsonl`).

### Validación de resultados

`make expected`: calcula las 5 consultas directamente sobre los mismos archivos y filas que manda el cliente, sin pasar por el sistema, y las escribe en `results/expected/` junto con sus hashes. Usa el mismo `DATASET_ROWS_PER_PATTERN` que el cliente, definido una sola vez en `x-dataset` del compose. Para comparar contra lo recién calculado: `make hashes EXPECTED=/results/expected`.

`make pack-expected`: comprime `results/expected/` en `expected/`, que es la carpeta versionada con los resultados esperados del dataset completo.

`make compare`: compara `results/client_0/` contra `expected/` y dice, por consulta, si coincide o qué filas faltan y sobran (`make compare CLIENT=client_1` para otro cliente, `make compare EXPECTED=/results/expected` para comparar contra lo recién calculado sin comprimir). Q4 se compara en orden y Q2 solo por nombre y url, porque el sistema devuelve la imagen y lo esperado su url.

`make hashes`: calcula un sha256 por consulta de `results/client_0/`, lo guarda en `results/client_0/hashes.txt` y lo muestra al lado del de `expected/hashes.txt`, diciendo si coincide.

`make expected-hashes`: regenera `expected/hashes.txt` a partir de `expected/`. Hace falta solo cuando cambian los esperados.

Los esperados son CSV y los resultados del cliente JSONL. Antes de comparar o calcular el hash, cada fila del cliente se pasa al formato del CSV: la fecha de Q1 de milisegundos a ISO en UTC y los números a texto. El hash de cada consulta es el sha256 de sus filas, sin encabezado, escritas como CSV y ordenadas (Q4 sin ordenar, porque el orden es parte de la respuesta).

### Subsets propios y varios clientes

`datasets/subset_a` y `datasets/subset_b` son dos subsets de 15 artículos elegidos a mano, uno por cliente, que cubren los casos de cada consulta (biografía con y sin imagen, antes y después de 2020, sin fecha, sin abstract, palabras compartidas entre idiomas). Cada uno tiene:

- `articles.txt`: la lista elegida, una línea por artículo con su archivo del dataset de Kaggle y su url.
- `articles.parquet`: los artículos tal cual vienen en Kaggle, que es lo que manda el cliente.
- `articles.json`: los mismos artículos en texto, para leerlos.
- `expected/`: el resultado esperado de cada consulta y sus hashes.

`make subsets`: arma `articles.parquet`, `articles.json` y `expected/` de cada subset a partir de su `articles.txt`. Hace falta solo cuando cambia la lista.

`make run-subsets`: corre el sistema con `client_0` mandando `subset_a` y `client_1` mandando `subset_b` al mismo tiempo (`docker-compose.subsets.yaml`), y termina cuando terminan los dos.

`make hashes-subsets`: compara los hashes de `results/client_0/` contra `subset_a` y los de `results/client_1/` contra `subset_b`.

Para ver el detalle de un cliente: la entrada en `datasets/subset_a/articles.json`, lo esperado en `datasets/subset_a/expected/` y lo obtenido en `results/client_0/`. `make compare CLIENT=client_1 EXPECTED=/datasets/subset_b/expected` muestra qué filas faltan o sobran.

Reglas que usa el cálculo directo: el período sale de `date_created` (sin fecha, el artículo no entra en Q1, Q2 ni Q5); las palabras son `\b\w+\b` en minúsculas, sin números ni stopwords de NLTK en inglés y francés, contadas una vez por abstract; Q5 compara la cantidad de palabras de cada abstract desde 2021 contra el promedio de los abstracts hasta 2020.

## Estructura

```
src/
  client/                 cliente
    main.py               configuracion, SIGTERM y arranque
    sender.py             arma los batches y los manda al gateway
    receiver.py           recibe Q1–Q5 mientras se envia la entrada y guarda resultados e imagenes
    dataset/
      kaggle.py           baja los archivos de Kaggle en un hilo aparte
      reader.py           lee las filas de cada archivo Parquet
      local.py            recorre una particion local sin modificar sus archivos
    Dockerfile
    requirements.txt
  gateway/                gateway
    main.py               configuracion, SIGTERM y arranque
    server.py             acepta clientes, se queda con las columnas necesarias y publica en clasificacion
    results.py            consume resultados, coordina los EOF por consulta y responde por el socket del cliente
    Dockerfile
    requirements.txt
  nodes/                  nodos de procesamiento
    classifier/
      main.py             configuracion, SIGTERM y arranque
      classifier.py       calcula periodo e idioma y reparte en texto, bio y resultados (Q1)
      Dockerfile
      requirements.txt
    filter_biography/
      main.py             configuracion, SIGTERM y arranque
      filter_biography.py  filtra las bios por biography o biographie y publica pedidos de imagenes
      Dockerfile
      requirements.txt
    image_downloader/
      main.py             configuracion, SIGTERM y arranque
      image_downloader.py descarga en un hilo aparte y publica cada resultado de Q2
      download.py         HTTP con limites, reintentos y cancelacion
      Dockerfile
      requirements.txt
    text_processor/
      main.py             configuracion, SIGTERM y arranque
      text_processor.py   consume texto, coordina los EOF de los clasificadores con los otros Text processor y da cada texto a las salidas
      words.py            saca las palabras de un abstract (minusculas, sin stopwords ni numeros)
      words_output.py     salida de Q3 y Q4: cuenta palabras por batch y las manda al contador que le toca por hash
      q5_output.py        salida de Q5: cuenta palabras distintas y separa estadisticas historicas y candidatos
      Dockerfile
      requirements.txt
    word_counter/
      main.py
      word_counter.py     guarda en disco las palabras de su shard en varios archivos y, al terminar un cliente, los procesa de a uno y manda las compartidas (Q3) y su top (Q4)
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
    average/
      main.py             configuracion, SIGTERM y arranque
      average.py          acumula las estadisticas por cliente y publica el promedio final exacto
      Dockerfile
      requirements.txt
    comparator/
      main.py             configuracion, SIGTERM y arranque
      comparator.py       guarda candidatos en disco y publica los que superan el promedio de Q5
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
      partition.py        hashes estables para repartir claves entre instancias y entre archivos de una instancia
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
  tools/
    expected/
      expected.py         calcula los resultados esperados de cada consulta directo sobre el dataset
      compare.py          compara los resultados de un cliente contra los esperados
      hashes.py           calcula un hash por consulta y lo compara contra los hashes esperados
      subset.py           arma un subset con los articulos elegidos a mano
      Dockerfile
      requirements.txt
datasets/                 subsets propios elegidos a mano, uno por cliente
expected/                 resultados esperados del dataset completo
tests/                    tests
docker-compose.subsets.yaml  agrega client_1 y hace que cada cliente mande su subset
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
| `LOCAL_DATASET_DIR` | Directorio Parquet local; si se indica, se usa en lugar de Kaggle | vacío |
| `RESULT_DIR` | Directorio de q1–q5.jsonl, imagenes y summary.json | `/tmp/results` |
| `RESULT_TIMEOUT_SECONDS` | Espera máxima de resultados despues de enviar toda la entrada | `7200` |

### Gateway

| Variable | Descripción | Por defecto |
|---|---|---|
| `SERVER_HOST` | Dirección donde escucha | `0.0.0.0` |
| `SERVER_PORT` | Puerto donde escucha | `5678` |
| `GATEWAY_ID` | Identificador del gateway | `0` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `CLASSIFICATION_QUEUE` | Cola donde publica los artículos | `clasificacion` |
| `RESULTS_QUEUE_PREFIX` | Cola consumida: prefijo seguido de `.<GATEWAY_ID>` | `resultados` |
| `CLASSIFIER_AMOUNT` | Origenes esperados para Q1 | `1` |
| `IMAGE_DOWNLOADER_AMOUNT` | Origenes esperados para Q2 | `1` |
| `COMPARATOR_AMOUNT` | Origenes esperados para Q5 | `1` |
| `PUBLISHER_IDLE_SECONDS` | Tiempo ocioso antes de renovar la conexion a clasificacion | `30` |

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
| `PUBLISHER_IDLE_SECONDS` | Tiempo ocioso antes de renovar conexiones de salida, incluido control | `30` |

Los publicadores abren la conexion al primer envio y la renuevan despues de inactividad. RabbitMQ confirma cada
publicacion y el enrutamiento es obligatorio. Un fallo de confirmacion o enrutamiento se propaga sin reintentar
automaticamente el mensaje: un broadcast parcial podria haber entregado algunos EOF. `make test-publisher`
verifica este comportamiento contra un broker real.

### Filter biography

| Variable | Descripción | Por defecto |
|---|---|---|
| `FILTER_BIOGRAPHY_ID` | Identificador del filtro | `0` |
| `FILTER_BIOGRAPHY_AMOUNT` | Cantidad de filtros | `1` |
| `CLASSIFIER_AMOUNT` | Cantidad de clasificadores, cuyos EOF espera | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `BIO_QUEUE` | Cola de candidatos de Q2 | `bio` |
| `IMAGES_QUEUE` | Cola compartida de pedidos de imagen | `imagenes` |
| `CONTROL_EXCHANGE` | Exchange para compartir EOF entre filtros | `control_filter_biography` |
| `MAX_BATCH_ROWS` | Maximo de pedidos por mensaje | `200` |
| `MAX_BATCH_BYTES` | Maximo de bytes del mensaje completo | `1048576` |
| `PUBLISHER_IDLE_SECONDS` | Tiempo ocioso antes de renovar una conexion de salida | `30` |

### Image downloader

| Variable | Descripción | Por defecto |
|---|---|---|
| `IMAGE_DOWNLOADER_ID` | Identificador del descargador | `0` |
| `IMAGE_DOWNLOADER_AMOUNT` | Cantidad de descargadores | `1` |
| `FILTER_BIOGRAPHY_AMOUNT` | Cantidad de filtros, cuyos EOF espera | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `IMAGES_QUEUE` | Cola compartida de pedidos de imagen | `imagenes` |
| `CONTROL_EXCHANGE` | Exchange para compartir EOF entre descargadores | `control_image_downloader` |
| `RESULTS_QUEUE_PREFIX` | Prefijo de las colas por gateway | `resultados` |
| `DOWNLOAD_TIMEOUT_SECONDS` | Timeout de las operaciones de red y presupuesto por intento, revisado entre lecturas | `10` |
| `DOWNLOAD_ATTEMPTS` | Intentos por imagen, incluyendo el primero | `3` |
| `DOWNLOAD_RETRY_SECONDS` | Espera inicial entre intentos; se duplica en cada reintento | `1` |
| `MAX_IMAGE_BYTES` | Maximo de bytes originales de una imagen | `1048576` |
| `MAX_RESULT_BYTES` | Maximo de bytes del resultado JSON completo | `8388608` |
| `PUBLISHER_IDLE_SECONDS` | Tiempo ocioso antes de renovar una conexion de salida | `30` |

### Text processor

| Variable | Descripción | Por defecto |
|---|---|---|
| `TEXT_PROCESSOR_ID` | Identificador del Text processor | `0` |
| `TEXT_PROCESSOR_AMOUNT` | Cantidad de Text processor | `1` |
| `CLASSIFIER_AMOUNT` | Cantidad de clasificadores, que es cuantos EOF espera | `1` |
| `WORD_COUNTER_AMOUNT` | Cantidad de contadores de palabras (shards) | `1` |
| `AVERAGE_AMOUNT` | Cantidad de nodos Average de Q5 | `1` |
| `COMPARATOR_AMOUNT` | Cantidad de comparadores de Q5 | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `TEXT_QUEUE` | Cola de la que consume | `texto` |
| `CONTROL_EXCHANGE` | Exchange por el que se reenvian los EOF de los clasificadores entre Text processor | `control_text_processor` |
| `WORDS_QUEUE_PREFIX` | Prefijo de las colas de los contadores, a las que se les agrega `.<shard>` | `palabras` |
| `AVERAGE_QUEUE_PREFIX` | Prefijo de las colas de estadisticas de Q5 por hash del cliente | `promedio` |
| `COMPARISON_QUEUE_PREFIX` | Prefijo de las colas de candidatos de Q5 por comparador | `comparacion` |
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
| `DATA_DIR` | Directorio donde guarda los archivos de cada cliente | `/data` |
| `WORD_COUNTER_BUCKETS` | Archivos por cliente entre los que reparte las palabras. Al terminar procesa uno a la vez, así que más archivos es menos memoria | `16` |
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

### Average (Q5)

| Variable | Descripción | Por defecto |
|---|---|---|
| `AVERAGE_ID` | Identificador del shard | `0` |
| `AVERAGE_AMOUNT` | Cantidad de nodos Average | `1` |
| `TEXT_PROCESSOR_AMOUNT` | Cantidad de Text processors cuyos EOF espera | `1` |
| `COMPARATOR_AMOUNT` | Cantidad de comparadores que reciben el promedio final | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `AVERAGE_QUEUE_PREFIX` | Prefijo de su cola de estadisticas | `promedio` |
| `FINAL_AVERAGE_QUEUE_PREFIX` | Prefijo de las colas durables por comparador para el promedio final | `promedio_final` |
| `PUBLISHER_IDLE_SECONDS` | Tiempo ocioso antes de renovar una conexion de salida | `30` |

### Comparator (Q5)

| Variable | Descripción | Por defecto |
|---|---|---|
| `COMPARATOR_ID` | Identificador del comparador | `0` |
| `COMPARATOR_AMOUNT` | Cantidad de comparadores | `1` |
| `TEXT_PROCESSOR_AMOUNT` | Cantidad de Text processors cuyos EOF espera | `1` |
| `RABBITMQ_HOST` | Host de RabbitMQ | `rabbitmq` |
| `COMPARISON_QUEUE_PREFIX` | Prefijo de su cola de candidatos | `comparacion` |
| `FINAL_AVERAGE_QUEUE_PREFIX` | Prefijo de su cola durable de promedios finales | `promedio_final` |
| `RESULTS_QUEUE_PREFIX` | Prefijo de las colas de resultados por gateway | `resultados` |
| `DATA_DIR` | Directorio de los archivos de candidatos pendientes | `/data` |
| `MAX_BATCH_ROWS` | Maximo de filas por resultado | `2000` |
| `MAX_BATCH_BYTES` | Maximo de bytes del mensaje completo de resultados | `1048576` |
| `DRAIN_BATCH_ROWS` | Maximo de candidatos leidos por paso al recorrer un archivo | `1000` |
| `PUBLISHER_IDLE_SECONDS` | Tiempo ocioso antes de renovar una conexion de salida | `30` |

## Q2

El clasificador publica en `bio` los articulos creados hasta 2020 con sections e image_url no nulos.
Filter biography busca `biography` o `biographie` sin distinguir mayusculas en el string completo de sections.
Para los que coinciden publica `ImageRequest(name, url, image_url)` en `imagenes`, conservando client_id y gateway_id.
Los mensajes respetan los limites de filas y de bytes, incluyendo el sobre JSON. Un pedido individual demasiado grande se descarta.

Image downloader procesa un batch por vez (prefetch 1), con un solo pedido HTTP activo por instancia en un hilo aparte.
El hilo del consumidor sigue atendiendo RabbitMQ y es el unico que publica, actualiza contadores y hace ack.
Cada descarga exitosa publica una fila `[name, url, image_bytes]` en `resultados.<gateway_id>`, con `query=2`.
image_bytes es una lista de enteros de 0 a 255 que conserva el archivo original. No se acumulan imagenes entre resultados.

HTTP usa la biblioteca estandar de Python y un User-Agent identificable, sigue redirects y exige Content-Type image/*.
Los timeouts, errores de conexion, HTTP 429 y 5xx se reintentan con espera exponencial y cantidad limitada de intentos.
Si el servidor indica `Retry-After`, se respeta esa espera; SIGTERM puede interrumpirla.
Las URL invalidas, otros errores HTTP, respuestas vacias o que no son imagenes y los excesos de tamanio se descartan.
El limite de bytes se aplica tambien sin Content-Length; los resultados tienen otro limite porque JSON ocupa mas que la imagen original.
SIGTERM cancela los reintentos y deja sin ack el batch que no termino. Un fallo de publicacion tambien deja el batch sin ack;
si vuelve a entregarse despues de una publicacion parcial, puede repetir resultados.

Los EOF y LATE_EOF se coordinan por etapa como en Text processor: cada replica comparte los avisos de la etapa anterior
y publica sus cantidades de mensajes procesados y enviados. Un descargador con trabajo activo difiere su EOF hasta terminarlo.
Los batches sin coincidencias o con todas las descargas fallidas igual cuentan como procesados y permiten terminar Q2 sin resultados.
Docker Compose configura dos filtros y dos descargadores. Al cambiar las cantidades hay que mantener consistentes los IDs y las cantidades de cada etapa.

El gateway consume `resultados.<gateway_id>` y envia cada resultado al cliente por su conexion TCP original.

## Q3 y Q4

Q3 y Q4 comparten el Text processor con Q5. El Text processor saca las palabras de cada abstract una sola vez (igual que el notebook de referencia: minusculas, `\b\w+\b`, sin las stopwords de NLTK en ingles y frances ni numeros) y se las da a la salida de cada consulta. Cada salida implementa:

- `send_batch(client_id, gateway_id, texts) -> {cola: mensajes mandados}`: recibe una lista de `ProcessedText(url, until_2020, language, words)`, donde `words` es la lista de palabras del abstract con repetidas, y publica lo que necesita su consulta.
- `send_eofs(client_id, sent, encode)`: manda `encode(cantidad, query=None)` a sus destinos, con la cantidad de mensajes que se le mando a cada uno. client_id permite elegir el shard de Average aunque no haya recibido datos historicos.
- `close()`.

La salida de Q3 y Q4 es `WordsOutput`. Q5 usa `Q5Output`, registrada junto a ella en `text_processor/main.py`. Los tipos de mensaje 7 a 9 de `internal.py` son de Q3 y Q4; los tipos 11 a 13 son de Q5.

Cada palabra va al contador `shard_of(palabra, WORD_COUNTER_AMOUNT)`. El contador guarda en disco lo que le llega de cada cliente, repartido en `WORD_COUNTER_BUCKETS` archivos según `bucket_of(palabra, WORD_COUNTER_BUCKETS)`, así todas las apariciones de una palabra quedan en el mismo archivo. Cuando terminan los datos, procesa los archivos de a uno (en memoria hay un solo archivo a la vez) y manda las palabras que aparecen en artículos en inglés y en francés al Join Q3 y sus `TOP_SIZE` palabras con más artículos al Top 20 final, ambos elegidos por `shard_of(client_id, cantidad)`. El Join Q3 publica las palabras en `resultados.<gateway_id>` como Q3, y el Top 20 final publica las 20 primeras, con empate por orden alfabetico, como Q4.

### EOF

Cada EOF trae `processed`, `total` y `sent`, y despues de mandarlo un nodo avisa con `LATE_EOF` lo que procese de mas. `common/eof/UpstreamTracker` los junta:

- El Text processor comparte una cola con los demas, asi que reenvia cada EOF de los clasificadores a todos por el exchange de control. Cuando tiene el de todos los clasificadores y lo procesado por ellos es igual al total, sabe cuantos textos hubo y manda su EOF a todos los contadores.
- Los contadores, el Join Q3 y el Top 20 final tienen una cola propia por instancia. Terminan a un cliente cuando llegaron los EOF de todos los nodos de la etapa anterior, la suma de lo procesado es igual al total y recibieron todos los mensajes que esos nodos dicen haber mandado.

## Q5

Q5 devuelve `[url, word_count]` de los articulos creados desde 2021 cuyo abstract tiene mas palabras distintas
que el promedio de los articulos creados hasta 2020 del mismo cliente. Ambas ramas usan `len(set(text.words))`,
despues de la limpieza compartida del Text processor. Las fechas desconocidas se ignoran; un abstract vacio
cuenta como cero palabras y, si es historico, igual suma un articulo al denominador. El promedio incluye los idiomas de entrada juntos.

`Q5Output` publica un `PartialAverage(total_words, article_count)` por batch con articulos historicos en
`promedio.<shard_of(client_id, AVERAGE_AMOUNT)>`. Los candidatos `CandidateArticle(url, word_count)` van en batches
por round-robin a `comparacion.<id>`. Los limites de Q5 incluyen el mensaje JSON completo; una fila individual
demasiado grande se descarta con un aviso. Las estadisticas no se descartan: un limite insuficiente detiene el procesamiento.

Cada Text processor manda EOF (y LATE_EOF cuando corresponde) al Average del cliente y a todos los comparadores,
incluyendo destinos con cero mensajes. Average espera todos los origenes, la igualdad de processed y total y
todos los mensajes anunciados en sent. Acumula sumas y cantidades, conserva el promedio exacto como
`FinalAverage(total_words, article_count)` y manda una copia a cada cola durable `promedio_final.<comparador>`.
Este broadcast usa colas propias por comparador, de manera que el promedio tambien queda disponible antes de que arranque su consumidor.

Comparator guarda candidatos en un archivo por cliente hasta conocer su promedio. Tiene un hilo para recibir promedios,
que programa su manejo en el hilo del consumidor de candidatos. Solo ese ultimo hilo toca el estado, los archivos y las salidas.
El archivo se recorre en pasos acotados por cantidad de candidatos y bytes, incluso cuando ninguno coincide,
para seguir atendiendo RabbitMQ. Los candidatos nuevos se comparan directamente mientras se vacia el archivo anterior.
La comparacion exacta es `word_count * article_count > total_words`, siempre que article_count sea positivo.
Sin articulos historicos no hay resultados; un promedio cero con articulos historicos permite los conteos positivos.

El ack de un candidato ocurre despues de escribirlo o publicar sus resultados. El promedio se reconoce en su propia conexion,
despues de recorrer correctamente el archivo pendiente. Un fallo de disco o publicacion deja sin ack la entrega afectada;
al repetir trabajo despues de una publicacion parcial, puede repetir resultados. Los archivos pendientes se preservan al apagar el nodo.
Average y Comparator conservan hasta 1024 IDs de clientes terminados para descartar duplicados recientes; los datos completos del cliente se liberan.

Cada Comparator manda su EOF con `query=5` cuando tiene el promedio final, termino de vaciar el archivo y completo los
EOF y conteos de todos los Text processors. En ese EOF, processed y total son cantidades locales de batches de candidatos;
sent es la cantidad de mensajes de resultados publicados por esa instancia. El consumidor del gateway espera
todos los comparadores y verificar los resultados recibidos contra la suma de sent, sin tratar el total local de un comparador
como un total global (`UpstreamTracker(..., check_total=False, check_received=True)`).

Docker Compose configura dos Average y dos Comparator. Las cantidades deben coincidir en los Text processors y en los nodos de Q5.
Los resultados se publican en `resultados.<gateway_id>` y el gateway los reenvia al cliente.

Los tests `test_q5_output.py`, `test_average.py`, `test_comparator.py` y `test_q5_pipeline.py` cubren los componentes y
la comparacion contra un calculo directo, con varios clientes, replicas, ordenes de llegada y batches tardios.
`make test` ejecuta estos tests junto al resto del repositorio.

## Entrega de resultados

El cliente recibe en un hilo mientras otro envia articulos. El gateway mantiene la conexion despues de `END_OF_DATA`,
publica el EOF de entrada y espera los cinco finales. Un consumidor de resultados por gateway enruta por client_id.
Q1 y Q2 verifican processed/total y mensajes recibidos/sent; Q3 y Q4 esperan el unico nodo final del cliente;
Q5 espera todos los comparadores y sus cantidades locales de sent.

El protocolo externo conserva ARTICLES=1 y END_OF_DATA=2 y agrega RESULTS=3 (`{query, rows}`),
QUERY_DONE=4 (`{query}`), ALL_DONE=5 y ERROR=6. ALL_DONE solo llega despues del fin de entrada y de las cinco consultas.
El gateway cierra entonces el socket; el cliente exige ese cierre y falla ante resultados posteriores o finales incompletos.

Los resultados se escriben progresivamente en `q1.jsonl` a `q5.jsonl`. Q2 recibe el array original de bytes,
guarda cada imagen en `images/<sha256>.bin` y escribe `[name, url, sha256, cantidad_bytes]` en su JSONL.
Al terminar correctamente se escribe `summary.json`, con filas enviadas, filas recibidas y consultas completas.
Para conservar resultados de un cliente normal en Docker hay que montar `RESULT_DIR` en el host. El compose ya lo hace con `client_0` en `results/client_0/`.

