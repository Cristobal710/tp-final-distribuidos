from .batch_builder import BatchBuilder


# Parte filas ya serializadas en grupos que respetan el tope de cantidad y de bytes.
def chunk_rows(serialized_rows, max_rows, max_bytes):
    builder = BatchBuilder(max_rows, max_bytes)
    for row in serialized_rows:
        closed = builder.add(row)
        if closed is not None:
            yield closed
    last = builder.flush()
    if last is not None:
        yield last
