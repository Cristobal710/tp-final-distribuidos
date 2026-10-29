# Suite completa con un cliente

Desde la raiz del repositorio:

```bash
make test-full
```

El host necesita Python 3, Docker Compose, acceso a Internet y espacio para la particion, los archivos temporales,
los resultados y las imagenes Docker. Las dependencias Python del procesamiento se ejecutan dentro de las imagenes
del proyecto. El ZIP de entrada se monta en modo lectura y no se extrae completo.

Para otra ubicacion o un plazo mayor por etapa:

```bash
python3 -m tests.full_suite.run \
  --archive /ruta/distr_dataset.zip \
  --artifacts /ruta/artefactos \
  --timeout 14400
```

`make test` omite esta prueba pesada. `tests/test_results_delivery.py` y `tests/test_full_suite_reference.py`
verifican rapidamente la entrega y el comparador, incluyendo duplicados, imagenes corruptas y orden de Q4.
`tests/test_reference_scenarios.py` verifica expectativas escritas a mano para fechas limite, entradas vacias,
palabras repetidas, filtros de Q2, empates de Q4 y casos de Q5 sin historicos, con promedio cero o igualdad exacta.
Estos casos pequenos complementan la particion real; no implican que todos aparezcan en sus 2 GB.

`make test-publisher` usa un RabbitMQ aislado para verificar publicaciones despues de inactividad, EOF enviados
por primera vez, confirmaciones del broker y fallos de enrutamiento sin repetir un broadcast parcial.

## Datos y expectativas

La particion apunta a 2.000.000.000 bytes Parquet, con tolerancia de 2 MiB por limites de los grupos de filas.
Se reparte aproximadamente mitad ingles y mitad frances. Proyecta las ocho columnas de entrada y conserva sus
valores originales. No modifica articulos ni repite filas para llenar espacio. Excluye registros cuyos campos
de texto principales superan 256 KiB para respetar los limites del transporte.

Un orden de archivos con semilla fija selecciona articulos anteriores y posteriores a 2021, fechas y abstracts
faltantes, palabras comunes y exclusivas y candidatos de Q5 por ambos lados del promedio. Selecciona 5 articulos
por idioma con imagenes reales descargables para Q2 y excluye los demas candidatos de Q2 para acotar el trafico.
La eleccion inicial depende de la disponibilidad HTTP. Una vez creado, el manifest congela exactamente la particion:
lista partes, tamanios, conteos y SHA-256, y `provenance.jsonl` identifica cada fila dentro del ZIP original.

Los metadatos de los probes HTTP se conservan en `image-selection.jsonl` para no repetir toda la seleccion si falla.
Al terminar cada idioma se guarda un checkpoint. Si falla el siguiente idioma, se verifican y conservan las
partes completas y se vuelve a seleccionar solo el idioma pendiente. Sin manifest ni checkpoint, una particion
esta incompleta y se debe quitar su directorio `dataset` o usar otro directorio de artefactos.
Una particion completa se reutiliza solo despues de verificar los checksums.
Tambien se verifican los CRC de los miembros originales del ZIP antes de reutilizarla.

Antes de iniciar la aplicacion, el oracle recorre la particion con PyArrow y SQLite. No importa los clasificadores
ni funciones de consulta de produccion. Congela las stopwords de la imagen del Text processor, cuenta palabras
distintas para Q5 y compara exactamente contra el promedio como fraccion entera. Verifica cobertura positiva de
las cinco consultas, negativas de Q3/Q5 y 20 resultados de Q4.
Ademas exporta `expected/q1.jsonl` hasta `q5.jsonl`, incluidos los duplicados esperados. `expected/scenarios.json`
indica el conteo y ejemplos por escenario: `covered` si aparece o `absent_from_this_partition` si no aparece.
Por ejemplo, una particion con promedio historico no entero no puede cubrir un conteo exactamente igual al promedio;
esa regla se verifica con un caso pequeno de expectativa conocida.

Q2 usa descargas HTTP reales tanto para el oracle como para los nodos. Se comparan SHA-256 y cantidad de bytes
de cada archivo recibido. No hay servidor de imagenes grabadas. Se respeta Retry-After, por lo que una limitacion
del servidor puede extender la ejecucion varios minutos. Una caida HTTP persistente, una imagen modificada o un
resultado faltante hacen fallar la prueba; las expectativas nunca se ajustan a la salida de la aplicacion.

## Ejecucion y comparacion

Cada ejecucion crea un proyecto Compose aislado sin puertos publicados. Espera todos los consumidores y bindings
de control, y despues arranca exactamente un cliente real con la particion montada en modo lectura.
Las comprobaciones de disponibilidad no abren sesiones de cliente. Los resultados regresan por el mismo socket
TCP mientras continua el envio de articulos. Se exige QUERY_DONE para Q1–Q5, ALL_DONE y cierre del socket.
Mientras el cliente corre, el runner revisa los workers cada dos segundos y muestra los conteos recibidos cada
30 segundos. Si un worker termina o queda unhealthy, interrumpe la prueba y conserva el diagnostico.

Q1, Q2, Q3 y Q5 se comparan como multiconjuntos completos: un resultado duplicado, faltante o adicional falla.
Q4 debe coincidir tambien en el orden, incluida la regla de desempate alfabetico. Se verifican los archivos de
imagen recibidos y el conteo de filas enviado por el cliente. Los logs deben mostrar una sola sesion aceptada.
Tambien se exige que las colas queden vacias y que el gateway no descarte resultados fuera de una sesion activa.

Los contenedores y volumenes del proyecto aislado se eliminan al finalizar. Los artefactos permanecen en
`.artifacts/full-suite/`, ignorado por Git:

- `dataset/`: particion, manifest y procedencia.
- `runs/<fecha>/expected/`: resultados SQLite y JSONL de Q1–Q5, imagenes esperadas, resumen y `scenarios.json`.
- `runs/<fecha>/actual/`: JSONL e imagenes recibidos por el cliente y su resumen.
- `runs/<fecha>/report.json`: comparacion por consulta, con ejemplos de diferencias si las hay.
- `runs/<fecha>/client.log`, `containers.log`, `queues.txt`: diagnostico del sistema real.
- `latest.json`: enlace al ultimo informe exitoso.

Las opciones `--target-bytes` y `--images-per-language` permiten probar el runner con un conjunto menor en otro
directorio de artefactos. Una ejecucion reducida no reemplaza la prueba completa de 2 GB.
