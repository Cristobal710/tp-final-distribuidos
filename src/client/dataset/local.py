import json
from pathlib import Path

from .reader import read_rows


def local_rows(directory, columns=None, rows_per_read=2048):
    root = Path(directory)
    manifest_path = root / "manifest.json"
    if manifest_path.is_file():
        names = [part["file"] for part in json.loads(manifest_path.read_text())["parts"]]
        files = [root / name for name in names]
        if any(path.resolve().parent != root.resolve() for path in files):
            raise ValueError("Los archivos del manifest tienen que estar dentro del directorio local")
    else:
        files = sorted(root.glob("*.parquet"))
    if not files:
        raise ValueError("No hay archivos Parquet locales")
    for path in files:
        yield from read_rows(path, columns, rows_per_read)
