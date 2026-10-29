import logging
import math
import threading
import time
from http.client import HTTPException, IncompleteRead
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

CHUNK_BYTES = 64 * 1024
USER_AGENT = "WikipediaQ2/1.0 (distributed article analysis)"


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


def _read_image(response, deadline, max_bytes, stop_event):
    if not response.headers.get_content_type().lower().startswith("image/"):
        raise DownloadError("La respuesta no es una imagen")
    declared = response.headers.get("Content-Length")
    try:
        declared = int(declared) if declared is not None else None
    except ValueError as e:
        raise DownloadError("Content-Length invalido") from e
    if declared is not None and (declared < 0 or declared > max_bytes):
        raise DownloadError("La imagen supera el limite de bytes")
    data = bytearray()
    while True:
        _check_cancelled(stop_event)
        if time.monotonic() >= deadline:
            raise TimeoutError("Se agoto el tiempo de descarga")
        # read1 permite revisar el plazo entre lecturas, incluso si el servidor manda datos de a poco.
        chunk = response.read1(min(CHUNK_BYTES, max_bytes - len(data) + 1))
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > max_bytes:
            raise DownloadError("La imagen supera el limite de bytes")
    if declared is not None and len(data) < declared:
        raise IncompleteRead(bytes(data), declared - len(data))
    if not data:
        raise DownloadError("La imagen esta vacia")
    return bytes(data)


def download_image(image_url, *, timeout, attempts, max_bytes, stop_event=None, retry_delay=1.0):
    if not math.isfinite(timeout) or timeout <= 0 or attempts < 1 or max_bytes < 1:
        raise ValueError("Los limites de descarga tienen que ser positivos y finitos")
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
    for attempt in range(attempts):
        _check_cancelled(stop_event)
        delay = retry_delay * (2 ** attempt)
        try:
            deadline = time.monotonic() + timeout
            with urlopen(request, timeout=timeout) as response:
                return _read_image(response, deadline, max_bytes, stop_event)
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
        if attempt + 1 < attempts:
            logging.warning("Descarga fallida (%s/%s): %s", attempt + 1, attempts, error)
            if stop_event.wait(delay):
                raise DownloadCancelled()
    raise DownloadError(f"Descarga fallida despues de {attempts} intentos: {error}") from error
