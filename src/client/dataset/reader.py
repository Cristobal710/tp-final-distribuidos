import pyarrow.parquet as pq

ROWS_PER_READ = 2048


# Recorre un archivo Parquet de a bloques y devuelve cada fila tal cual, solo con las columnas pedidas o con todas.
def read_rows(path, columns=None, rows_per_read=ROWS_PER_READ):
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=rows_per_read, columns=columns):
        yield from batch.to_pylist()
