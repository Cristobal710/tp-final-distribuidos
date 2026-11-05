import csv
import hashlib
import json
from pathlib import Path


def cached_image_path(directory, url):
    return Path(directory) / (hashlib.sha256(url.encode("utf-8")).hexdigest() + ".img")


def validate_images(directory, expected_csv, cache_directory=None):
    directory = Path(directory)
    with Path(expected_csv).open(newline="") as source:
        image_urls = {row["url"]: row["image_url"] for row in csv.DictReader(source)}
    problems = []
    count = 0
    results = directory / "q2.jsonl"
    if not results.exists():
        return {"passed": False, "images_checked": 0, "problems": ["Missing Q2 result file"]}
    for line in results.read_text().splitlines():
        if not line.strip():
            continue
        count += 1
        try:
            _, url, digest, size = json.loads(line)
            if url not in image_urls:
                raise ValueError(f"Unexpected article URL: {url}")
            if (not isinstance(digest, str) or len(digest) != 64
                    or any(character not in "0123456789abcdef" for character in digest)):
                raise ValueError(f"Invalid image digest for {url}")
            data = (directory / "images" / f"{digest}.bin").read_bytes()
            if not data or len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                raise ValueError(f"Corrupt received image: {url}")
            if cache_directory is not None:
                cached = cached_image_path(cache_directory, image_urls[url]).read_bytes()
                if cached != data:
                    raise ValueError(f"Cached and received image differ: {url}")
        except (OSError, ValueError, TypeError) as error:
            problems.append(str(error))
    return {"passed": not problems, "images_checked": count, "problems": problems}
