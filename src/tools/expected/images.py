import csv
import hashlib
import os
import sys
import time
from http.client import HTTPException
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from compare import IMAGES_FILE, read_rows

USER_AGENT = "TPDistribuidosFIUBA/1.0 (https://github.com/Cristobal710/tp-final-distribuidos)"
ATTEMPTS = 10
TIMEOUT_SECONDS = 60
WAIT_SECONDS = 2
Q2_HEADER = ["name", "url", "image_url"]
FAILED_DOWNLOAD = (OSError, HTTPException, RuntimeError, ValueError)


def retry_after(error):
    value = error.headers.get("Retry-After", "")
    return int(value) if value.isdigit() else WAIT_SECONDS


def download(url, opener=urlopen, sleep=time.sleep):
    for _ in range(ATTEMPTS):
        try:
            with opener(Request(url, headers={"User-Agent": USER_AGENT}), timeout=TIMEOUT_SECONDS) as response:
                return response.read()
        except HTTPError as error:
            if error.code != 429 and error.code < 500:
                raise
            sleep(retry_after(error))
        except (OSError, HTTPException):
            sleep(WAIT_SECONDS)
    raise RuntimeError(f"No se pudo bajar {url} despues de {ATTEMPTS} intentos")


def write_image_hashes(directory, fetch, sleep=time.sleep):
    path = os.path.join(directory, "q2.csv")
    rows = read_rows(path, 2)
    available = []
    with open(os.path.join(directory, IMAGES_FILE), "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["url", "image_sha256"])
        for row in rows:
            _, url, image_url = row
            try:
                image = fetch(image_url)
            except FAILED_DOWNLOAD as error:
                print(f"Imagen descartada {image_url}: {error}", flush=True)
            else:
                writer.writerow([url, hashlib.sha256(image).hexdigest()])
                available.append(row)
            sleep(WAIT_SECONDS)
    if len(available) != len(rows):
        with open(path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(Q2_HEADER)
            writer.writerows(available)


def main():
    if len(sys.argv) != 2:
        print("Uso: images.py <directorio de esperados>")
        return 2
    write_image_hashes(sys.argv[1], download)
    return 0


if __name__ == "__main__":
    sys.exit(main())
