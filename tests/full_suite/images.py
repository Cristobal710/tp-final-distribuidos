"""Independent live-image reference, deliberately separate from the application downloader."""
import hashlib
import math
import os
import threading
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from common.request_throttle import RequestThrottle

USER_AGENT = (
    "DistributedWikipediaFullSuiteBot/1.0 "
    "(https://github.com/Cristobal710/tp-final-distribuidos) "
    f"python-requests/{requests.__version__}"
)
THROTTLE = RequestThrottle(float(os.environ.get("DOWNLOAD_INTERVAL_SECONDS", "0")))


def retry_after(value):
    try:
        return max(0, int(value))
    except (ValueError, TypeError):
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, AttributeError):
            return 0


def snapshot_image(url, timeout=20, stop_event=None):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("The image timeout must be finite and positive")
    stop_event = stop_event if stop_event is not None else threading.Event()
    attempt, backoff = 0, 1
    while True:
        if stop_event.is_set() or not THROTTLE.wait(stop_event):
            raise InterruptedError(f"Live image reference cancelled: {url}")
        attempt += 1
        delay = backoff
        try:
            with requests.get(url, stream=True, timeout=timeout,
                              headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}) as response:
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    delay = max(delay, retry_after(response.headers.get("Retry-After")))
                    response.raise_for_status()
                if not response.ok:
                    return {"success": False, "reason": f"HTTP {response.status_code}"}
                content_type = response.headers.get("Content-Type", "").split(";")[0].lower()
                if not content_type.startswith("image/"):
                    return {"success": False, "reason": "not an image"}
                digest, size = hashlib.sha256(), 0
                for chunk in response.iter_content(65536):
                    if stop_event.is_set():
                        raise InterruptedError(f"Live image reference cancelled: {url}")
                    size += len(chunk)
                    digest.update(chunk)
                if not size:
                    return {"success": False, "reason": "empty image"}
                return {"success": True, "sha256": digest.hexdigest(), "bytes": size,
                        "content_type": content_type, "etag": response.headers.get("ETag"),
                        "last_modified": response.headers.get("Last-Modified"),
                        "fetched_at": datetime.now(timezone.utc).isoformat()}
        except requests.RequestException as error:
            print(f"Live image retry in {delay:g}s (attempt {attempt + 1})", flush=True)
            if stop_event.wait(delay):
                raise InterruptedError(f"Live image reference cancelled: {url}") from error
            backoff = min(backoff * 2, 60)

