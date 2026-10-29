"""Independent live-image reference, deliberately separate from the application downloader."""
import hashlib
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

MAX_IMAGE_BYTES = 1048576
USER_AGENT = "DistributedWikipediaFullSuite/1.0 (local integration test)"


class ImageHostRateLimited(RuntimeError):
    def __init__(self, message, retry_after_seconds=0):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


def retry_after(value):
    try:
        return max(0, int(value))
    except (ValueError, TypeError):
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, AttributeError):
            return 0


def snapshot_image(url, attempts=3, timeout=20, max_bytes=MAX_IMAGE_BYTES):
    for attempt in range(attempts):
        delay = 2 ** attempt
        try:
            with requests.get(url, stream=True, timeout=timeout,
                              headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}) as response:
                if response.status_code == 429 or response.status_code >= 500:
                    delay = max(delay, retry_after(response.headers.get("Retry-After")))
                    response.raise_for_status()
                if not response.ok:
                    return {"success": False, "reason": f"HTTP {response.status_code}"}
                content_type = response.headers.get("Content-Type", "").split(";")[0].lower()
                if not content_type.startswith("image/"):
                    return {"success": False, "reason": "not an image"}
                if int(response.headers.get("Content-Length", 0)) > max_bytes:
                    return {"success": False, "reason": "image too large"}
                digest, size = hashlib.sha256(), 0
                for chunk in response.iter_content(65536):
                    size += len(chunk)
                    if size > max_bytes:
                        return {"success": False, "reason": "image too large"}
                    digest.update(chunk)
                if not size:
                    return {"success": False, "reason": "empty image"}
                return {"success": True, "sha256": digest.hexdigest(), "bytes": size,
                        "content_type": content_type, "etag": response.headers.get("ETag"),
                        "last_modified": response.headers.get("Last-Modified"),
                        "fetched_at": datetime.now(timezone.utc).isoformat()}
        except requests.RequestException as error:
            if attempt + 1 == attempts:
                if (error.response is not None and error.response.status_code == 429
                        and retry_after(error.response.headers.get("Retry-After")) > 0):
                    raise ImageHostRateLimited(f"Image host is still rate limiting after retries: {url}",
                                               retry_after(error.response.headers.get("Retry-After"))) from error
                raise RuntimeError(f"Live image preflight failed for {url}: {error}") from error
            print(f"Live image retry in {delay:g}s (attempt {attempt + 2}/{attempts})", flush=True)
            time.sleep(delay)

