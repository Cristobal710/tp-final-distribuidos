import logging
import math
import sys
import threading
import time
from http.client import HTTPException, IncompleteRead
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

CHUNK_BYTES = 64 * 1024
MAX_RETRY_DELAY_SECONDS = 60
USER_AGENT = (
    "WikipediaQ2Bot/1.0 "
    "(https://github.com/Cristobal710/tp-final-distribuidos) "
    f"Python-urllib/{sys.version_info.major}.{sys.version_info.minor}"
)


class DownloadError(Exception):
    pass


class DownloadCancelled(Exception):
    pass


def _retry_after(headers):
    value = headers.get("Retry-After") if headers is not None else None
    try:
        return max(0, int(value))
    except (ValueError, TypeError):
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, AttributeError):
            return 0


def _check_cancelled(stop_event):
    if stop_event.is_set():
        raise DownloadCancelled()


def _read_image(response, deadline, stop_event):
    if not response.headers.get_content_type().lower().startswith("image/"):
        raise DownloadError("La respuesta no es una imagen")
    declared = response.headers.get("Content-Length")
    try:
        declared = int(declared) if declared is not None else None
    except ValueError as e:
        raise DownloadError("Content-Length invalido") from e
    if declared is not None and declared < 0:
        raise DownloadError("Content-Length invalido")
    data = bytearray()
    while True:
        _check_cancelled(stop_event)
        if time.monotonic() >= deadline:
            raise TimeoutError("Se agoto el tiempo de descarga")
        # read1 permite revisar el plazo entre lecturas, incluso si el servidor manda datos de a poco.
        chunk = response.read1(CHUNK_BYTES)
        if not chunk:
            break
        data.extend(chunk)
    if declared is not None and len(data) < declared:
        raise IncompleteRead(bytes(data), declared - len(data))
    if not data:
        raise DownloadError("La imagen esta vacia")
    return bytes(data)


def download_image(image_url, *, timeout, stop_event=None, retry_delay=1.0,
                   throttle=None, cache=None):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("El timeout tiene que ser finito y positivo")
    if not math.isfinite(retry_delay) or retry_delay < 0:
        raise ValueError("La espera entre reintentos tiene que ser finita y no negativa")
    stop_event = stop_event if stop_event is not None else threading.Event()
    _check_cancelled(stop_event)
    try:
        if not isinstance(image_url, str) or not image_url.strip():
            raise ValueError("Falta la URL")
        parsed = urlsplit(image_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("Se necesita una URL HTTP o HTTPS")
        parsed.port
        request = Request(image_url, headers={"User-Agent": USER_AGENT})
    except ValueError as e:
        raise DownloadError(f"URL de imagen invalida: {e}") from e
    if cache is not None:
        cached = cache.get(image_url)
        if cached is not None:
            return cached
    attempt = 0
    backoff = retry_delay
    max_backoff = max(retry_delay, MAX_RETRY_DELAY_SECONDS)
    while True:
        attempt += 1
        _check_cancelled(stop_event)
        if throttle is not None and not throttle.wait(stop_event):
            raise DownloadCancelled()
        delay = backoff
        try:
            deadline = time.monotonic() + timeout
            with urlopen(request, timeout=timeout) as response:
                image = _read_image(response, deadline, stop_event)
            if cache is not None:
                cache.put(image_url, image)
            return image
        except HTTPError as e:
            delay = max(delay, _retry_after(e.headers))
            e.close()
            if e.code != 429 and not 500 <= e.code < 600:
                raise DownloadError(f"HTTP {e.code}") from e
            error = e
        except (URLError, OSError, HTTPException) as e:
            error = e
        except ValueError as e:
            raise DownloadError(f"Solicitud de imagen invalida: {e}") from e
        logging.warning("Descarga fallida (intento %s): %s", attempt, error)
        if stop_event.wait(delay):
            raise DownloadCancelled()
        backoff = min(backoff * 2, max_backoff)
