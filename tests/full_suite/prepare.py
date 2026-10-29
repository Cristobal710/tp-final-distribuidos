"""Build a bounded, reproducible partition from original rows in distr_dataset.zip."""
import argparse
import hashlib
import json
import random
import shutil
import tempfile
import time
from collections import Counter
from pathlib import Path
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq

from tests.full_suite.images import ImageHostRateLimited, snapshot_image

COLUMNS = ["name", "url", "in_language", "date_created", "date_modified", "abstract", "sections", "image"]
TARGET_BYTES = 2_000_000_000
SEED = 271828


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def historical(row):
    created = row["date_created"]
    return created is not None and created.year <= 2020


def image_candidate(row):
    sections = row["sections"]
    url = (row["image"] or {}).get("content_url")
    return historical(row) and url is not None and isinstance(sections, str) and (
        "biography" in sections.casefold() or "biographie" in sections.casefold())


def prepare(archive_path, output_dir, target_bytes=TARGET_BYTES, images_per_language=5):
    if target_bytes <= 0 or images_per_language < 1:
        raise ValueError("Partition size and image coverage must be positive")
    archive_path, output_dir = Path(archive_path), Path(output_dir)
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text())
        if (existing["target_bytes"] == target_bytes and existing["images_per_language"] == images_per_language
                and existing["archive_bytes"] == archive_path.stat().st_size):
            with ZipFile(archive_path) as archive:
                for member in existing["source_members"]:
                    if archive.getinfo(member["file"]).CRC != member["crc32"]:
                        raise RuntimeError(f"Source archive member changed: {member['file']}")
            for part in existing["parts"]:
                if sha256_file(output_dir / part["file"]) != part["sha256"]:
                    raise RuntimeError(f"Partition checksum mismatch: {part['file']}")
            print(f"Reusing verified partition: {existing['bytes']} bytes, {existing['rows']} rows", flush=True)
            return existing
        raise RuntimeError("Existing partition has different parameters; select a new artifact directory")
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text()) if checkpoint_path.exists() else None
    if checkpoint is None and any(output_dir.glob("*.parquet")):
        raise RuntimeError("Incomplete partition exists; remove it or select a new artifact directory")
    cache_path = output_dir.parent / "image-selection.jsonl"
    image_snapshots = {}
    if cache_path.exists():
        for line in cache_path.read_text().splitlines():
            entry = json.loads(line)
            image_snapshots[entry["url"]] = entry["snapshot"]
    if checkpoint is not None:
        if (checkpoint["target_bytes"] != target_bytes or checkpoint["images_per_language"] != images_per_language
                or checkpoint["archive_bytes"] != archive_path.stat().st_size):
            raise RuntimeError("Checkpoint parameters differ; select a new artifact directory")
        counts, parts, source_members = Counter(checkpoint["coverage"]), checkpoint["parts"], checkpoint["source_members"]
        for part in parts:
            if sha256_file(output_dir / part["file"]) != part["sha256"]:
                raise RuntimeError(f"Checkpoint part changed: {part['file']}")
        complete = {part["file"] for part in parts}
        for path in output_dir.glob("part-*.parquet"):
            if path.name not in complete:
                path.unlink()
        provenance = open(output_dir / "provenance.jsonl", "r+")
        provenance.seek(checkpoint["provenance_bytes"])
        provenance.truncate()
        print(f"Resuming partition after {len(parts)} complete parts", flush=True)
    else:
        counts, parts, source_members = Counter(), [], []
        provenance = open(output_dir / "provenance.jsonl", "w")
    part_number = len(parts)
    try:
        with ZipFile(archive_path) as archive, tempfile.TemporaryDirectory(dir=output_dir, prefix="source-") as scratch:
            for member in source_members:
                if archive.getinfo(member["file"]).CRC != member["crc32"]:
                    raise RuntimeError(f"Checkpoint source member changed: {member['file']}")
            for language, priority in (("en", 198), ("fr", 77)):
                if counts[f"{language}_q2_images"] == images_per_language:
                    continue
                members = [e for e in archive.infolist()
                           if e.filename.startswith(f"{language}wiki/") and e.filename.endswith(".parquet")]
                order = list(range(len(members)))
                random.Random(SEED).shuffle(order)
                priority = min(priority, len(members) - 1)
                order.remove(priority)
                order.insert(0, priority)
                stored, language_rows, selected_images = 0, 0, 0
                image_attempts = 0
                sink = writer = part_path = None
                part_rows = 0
                quota = target_bytes // 2

                def close_part():
                    nonlocal sink, writer, stored, part_rows
                    if writer is None:
                        return
                    writer.close()
                    sink.close()
                    size = part_path.stat().st_size
                    parts.append({"file": part_path.name, "bytes": size, "rows": part_rows,
                                  "language": language, "sha256": sha256_file(part_path)})
                    stored += size
                    print(f"Partition {language}: {stored:,} bytes, {language_rows:,} rows, "
                          f"{selected_images} live images", flush=True)
                    writer = sink = None
                    part_rows = 0

                try:
                    for index in order:
                        member = members[index]
                        source_path = Path(scratch) / "member.parquet"
                        with archive.open(member) as source, open(source_path, "wb") as target:
                            shutil.copyfileobj(source, target, length=1024 * 1024)
                        source_members.append({"file": member.filename, "crc32": member.CRC,
                                               "sha256": sha256_file(source_path)})
                        parquet = pq.ParquetFile(source_path)
                        offset = 0
                        for batch in parquet.iter_batches(batch_size=256, columns=COLUMNS):
                            selected = []
                            for i, row in enumerate(batch.to_pylist()):
                                # Keep every selected field verbatim; select bounded original records for transport.
                                if sum(len((row.get(key) or "").encode("utf-8"))
                                       for key in ("sections", "abstract", "name", "url")) > 256 * 1024:
                                    counts["excluded_large_rows"] += 1
                                    continue
                                if image_candidate(row):
                                    if selected_images >= images_per_language:
                                        continue
                                    url = row["image"]["content_url"]
                                    if url not in image_snapshots:
                                        image_attempts += 1
                                        if image_attempts > 2000:
                                            raise RuntimeError(f"Unable to select {images_per_language} live {language} images")
                                        resume_at = max((s.get("retry_not_before", 0) for s in image_snapshots.values()), default=0)
                                        delay = max(1, resume_at - time.time())
                                        if delay > 1:
                                            print(f"Respecting image-host cooldown for {delay:.0f}s", flush=True)
                                        time.sleep(delay)
                                        try:
                                            image_snapshots[url] = snapshot_image(url)
                                        except ImageHostRateLimited as error:
                                            image_snapshots[url] = {"success": False, "reason": str(error),
                                                "retry_not_before": time.time() + error.retry_after_seconds}
                                            with open(cache_path, "a") as cache:
                                                cache.write(json.dumps({"url": url, "snapshot": image_snapshots[url]}) + "\n")
                                            raise
                                        except RuntimeError as error:
                                            # Selection is still open: exclude this original row before freezing expectations.
                                            image_snapshots[url] = {"success": False, "reason": str(error)}
                                        with open(cache_path, "a") as cache:
                                            cache.write(json.dumps({"url": url, "snapshot": image_snapshots[url]}) + "\n")
                                        if image_attempts % 25 == 0:
                                            print(f"Image selection: {image_attempts} probes, {selected_images} selected; "
                                                  f"{sum(s['success'] for s in image_snapshots.values())} usable URLs", flush=True)
                                    if not image_snapshots[url]["success"]:
                                        counts["excluded_unusable_images"] += 1
                                        continue
                                    selected_images += 1
                                    print(f"Selected {language} live image {selected_images}/{images_per_language}", flush=True)
                                selected.append(i)
                                created = row["date_created"]
                                counts[f"{language}_rows"] += 1
                                counts[f"{language}_historical" if historical(row) else
                                       f"{language}_recent" if created is not None else f"{language}_missing_date"] += 1
                                if row["abstract"] is None:
                                    counts["missing_abstract"] += 1
                            if selected:
                                table = pa.Table.from_batches([batch]).take(pa.array(selected, type=pa.int32()))
                                if writer is None:
                                    part_path = output_dir / f"part-{part_number:04d}-{language}.parquet"
                                    part_number += 1
                                    sink = open(part_path, "wb")
                                    writer = pq.ParquetWriter(sink, table.schema, compression="zstd", compression_level=3)
                                writer.write_table(table, row_group_size=len(selected))
                                part_rows += len(selected)
                                language_rows += len(selected)
                                provenance.write(json.dumps({"member": member.filename,
                                                             "rows": [offset + i for i in selected]}) + "\n")
                            offset += len(batch)
                            current = sink.tell() if sink is not None else 0
                            if current >= 32 * 1024 * 1024 or stored + current >= quota - 128 * 1024:
                                close_part()
                            if stored >= quota - 128 * 1024 and selected_images >= images_per_language:
                                break
                        parquet.close()
                        if stored >= quota - 128 * 1024 and selected_images >= images_per_language:
                            break
                    close_part()
                finally:
                    if writer is not None:
                        writer.close()
                        sink.close()
                if stored < quota - 1024 * 1024 or selected_images < images_per_language:
                    raise RuntimeError(f"Insufficient {language} data or live-image coverage")
                counts[f"{language}_q2_images"] = selected_images
                provenance.flush()
                checkpoint_temp = checkpoint_path.with_suffix(".tmp")
                checkpoint_temp.write_text(json.dumps({
                    "target_bytes": target_bytes, "images_per_language": images_per_language,
                    "archive_bytes": archive_path.stat().st_size, "coverage": dict(counts), "parts": parts,
                    "source_members": source_members, "provenance_bytes": provenance.tell(),
                }, indent=2))
                checkpoint_temp.replace(checkpoint_path)
        size = sum(part["bytes"] for part in parts)
        if abs(size - target_bytes) > max(2 * 1024 * 1024, target_bytes // 1000):
            raise RuntimeError(f"Partition size {size} is outside the target tolerance")
        manifest = {"version": 1, "seed": SEED, "target_bytes": target_bytes, "bytes": size,
                    "rows": sum(part["rows"] for part in parts), "images_per_language": images_per_language,
                    "archive": str(archive_path), "archive_bytes": archive_path.stat().st_size,
                    "columns": COLUMNS, "parts": parts, "coverage": dict(counts), "source_members": source_members}
        (output_dir / "preparation_images.json").write_text(json.dumps(image_snapshots, indent=2))
        manifest_temp = manifest_path.with_suffix(".tmp")
        manifest_temp.write_text(json.dumps(manifest, indent=2))
        manifest_temp.replace(manifest_path)
        checkpoint_path.unlink(missing_ok=True)
        print(json.dumps({"partition_bytes": size, "rows": manifest["rows"], "coverage": dict(counts)}), flush=True)
        return manifest
    finally:
        provenance.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--target-bytes", type=int, default=TARGET_BYTES)
    parser.add_argument("--images-per-language", type=int, default=5)
    args = parser.parse_args()
    prepare(args.archive, args.output, args.target_bytes, args.images_per_language)
