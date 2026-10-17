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
  common/                 lo que comparten todos los nodos
    batching/
      batch_builder.py    junta items en batches con tope de cantidad y de bytes
    protocol/
      external.py         mensajes entre cliente y gateway y serializacion
      articles.py         formato de los batches de articulos
    safe_socket/
      safe_socket.py      lectura y escritura completas sobre sockets
tests/                    tests
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
| `CLIENT_COLUMNS` | Columnas a mandar, separadas por coma. Vacía manda todas | las 8 que usan las consultas |
| `PARQUET_ROWS_PER_READ` | Filas por bloque al leer cada archivo | `2048` |
| `MAX_BATCH_ROWS` | Máximo de filas por batch | `200` |
| `MAX_BATCH_BYTES` | Máximo de bytes por batch | `1048576` |
| `DOWNLOAD_DIR` | Directorio de las descargas | `/tmp/downloads` |
| `DOWNLOAD_FILES_AHEAD` | Archivos que se bajan por adelantado | `1` |
| `DOWNLOAD_POLL_SECONDS` | Espera entre intentos de dejar un archivo listo | `0.5` |
| `DOWNLOAD_STOP_TIMEOUT_SECONDS` | Espera máxima al hilo de descarga al terminar | `5` |
