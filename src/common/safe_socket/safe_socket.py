# Lee size bytes del socket, juntando los pedazos en que lleguen.
def recv_all(sock, size):
    buffer = bytearray(size)
    view = memoryview(buffer)
    received = 0
    while received < size:
        n = sock.recv_into(view[received:])
        if n == 0:
            raise ConnectionError(f"Se cerró la conexión con {size - received} bytes pendientes")
        received += n
    return bytes(buffer)


# Manda todos los bytes, reintentando con lo que send no llegue a mandar.
def send_all(sock, data):
    view = memoryview(data)
    sent = 0
    while sent < len(view):
        sent += sock.send(view[sent:])
