# Junta items en batches que se cierran al llegar a max_items o a max_bytes, sin partir items.
class BatchBuilder:

    def __init__(self, max_items, max_bytes):
        if max_items < 1 or max_bytes < 1:
            raise ValueError("max_items y max_bytes tienen que ser positivos")
        self._max_items = max_items
        self._max_bytes = max_bytes
        self._items = []
        self._size = 0

    # Agrega un item y, si tuvo que cerrar el batch actual, lo devuelve.
    def add(self, item):
        closed = None
        if self._items and self._size + len(item) > self._max_bytes:
            closed = self.flush()
        self._items.append(item)
        self._size += len(item)
        if len(self._items) >= self._max_items:
            closed = self.flush()
        return closed

    # Cierra el batch actual y devuelve sus items, o None si estaba vacio.
    def flush(self):
        if not self._items:
            return None
        items = self._items
        self._items = []
        self._size = 0
        return items
