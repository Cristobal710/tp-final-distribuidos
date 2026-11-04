import csv
import hashlib
import os
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from compare import IMAGES_FILE, read_rows

USER_AGENT = "TPDistribuidosFIUBA/1.0 (https://github.com/Cristobal710/tp-final-distribuidos)"
ATTEMPTS = 10
TIMEOUT_SECONDS = 60
WAIT_SECONDS = 2


def retry_after(error):
    value = error.headers.get("Retry-After", "")
    return int(value) if value.isdigit() else WAIT_SECONDS


def download(url, opener=urlopen, sleep=time.sleep):
    for _ in range(ATTEMPTS):
        try:
            with opener(Request(url, headers={"User-Agent": USER_AGENT}), timeout=TIMEOUT_SECONDS) as response:
                return response.read()
        except HTTPError as error:
            if error.code != 429:
                raise
            sleep(retry_after(error))
    raise RuntimeError(f"No se pudo bajar {url} despues de {ATTEMPTS} intentos")


def write_image_hashes(directory, fetch, sleep=time.sleep):
    rows = read_rows(os.path.join(directory, "q2.csv"), 2)
    with open(os.path.join(directory, IMAGES_FILE), "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["url", "image_sha256"])
        for _, url, image_url in rows:
            writer.writerow([url, hashlib.sha256(fetch(image_url)).hexdigest()])
            sleep(WAIT_SECONDS)


def main():
    if len(sys.argv) != 2:
        print("Uso: images.py <directorio de esperados>")
        return 2
    write_image_hashes(sys.argv[1], download)
    return 0


if __name__ == "__main__":
    sys.exit(main())
