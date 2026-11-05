import hashlib
import json
import time
from pathlib import Path
from unittest.mock import Mock, patch

from download import download_image
from image_cache import ImageCache


root = Path('/test_artifacts')
cache = ImageCache('/cache/q2')
cold = json.loads((root / 'cold/report.json').read_text())
started = time.monotonic()
checked = []
throttle = Mock()
throttle.wait.side_effect = AssertionError('A cache hit must not wait for the HTTP throttle')
with patch('download.urlopen', side_effect=AssertionError('A cache hit must not use HTTP')) as network:
    for client in cold['clients'].values():
        for _, article_url, digest, size in client['actual']:
            url = client['image_urls'][article_url]
            original = (root / 'cold' / f'{digest}.bin').read_bytes()
            data = download_image(url, timeout=10, throttle=throttle, cache=cache)
            assert data == original and len(data) == size
            assert hashlib.sha256(data).hexdigest() == digest
            checked.append({'url': url, 'bytes': size, 'sha256': digest})
    assert network.call_count == 0
report = {'passed': bool(checked), 'images_reused': len(checked), 'http_requests': 0,
          'elapsed_seconds': time.monotonic() - started, 'images': checked}
(root / 'warm-cache-check.json').write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2), flush=True)
