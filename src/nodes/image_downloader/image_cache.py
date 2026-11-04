import logging
import sqlite3
import time
from contextlib import closing
from pathlib import Path


class ImageCache:

    def __init__(self, directory, max_bytes):
        if max_bytes < 1:
            raise ValueError("El limite de la cache tiene que ser positivo")
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self._path = directory / "images.sqlite3"
        self._max_bytes = max_bytes
        with closing(sqlite3.connect(self._path)) as connection, connection:
            connection.execute("PRAGMA auto_vacuum = FULL")
            connection.execute("""CREATE TABLE IF NOT EXISTS images(
                url TEXT PRIMARY KEY, data BLOB NOT NULL, size INTEGER NOT NULL, used INTEGER NOT NULL
            )""")
            connection.execute("CREATE INDEX IF NOT EXISTS images_used ON images(used)")
            connection.execute("BEGIN IMMEDIATE")
            self._trim(connection, max_bytes)

    @staticmethod
    def _trim(connection, limit):
        total = connection.execute("SELECT COALESCE(SUM(size), 0) FROM images").fetchone()[0]
        while total > limit:
            url, size = connection.execute("SELECT url, size FROM images ORDER BY used LIMIT 1").fetchone()
            connection.execute("DELETE FROM images WHERE url = ?", (url,))
            total -= size

    def get(self, url, max_image_bytes=0):
        try:
            with closing(sqlite3.connect(self._path)) as connection, connection:
                row = connection.execute(
                    "SELECT data FROM images WHERE url = ? AND size > 0 AND (? = 0 OR size <= ?)",
                    (url, max_image_bytes, max_image_bytes),
                ).fetchone()
                if row is not None:
                    connection.execute("UPDATE images SET used = ? WHERE url = ?", (time.time_ns(), url))
                    return row[0]
        except (OSError, sqlite3.Error):
            logging.warning("No se pudo leer la cache de imagenes", exc_info=True)
        return None

    def put(self, url, data):
        if not data or len(data) > self._max_bytes:
            return
        try:
            with closing(sqlite3.connect(self._path)) as connection, connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("DELETE FROM images WHERE url = ?", (url,))
                self._trim(connection, self._max_bytes - len(data))
                connection.execute("INSERT INTO images VALUES (?, ?, ?, ?)",
                                   (url, data, len(data), time.time_ns()))
        except (OSError, sqlite3.Error):
            logging.warning("No se pudo guardar la imagen en cache", exc_info=True)
