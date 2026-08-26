"""Path conventions and file writes.

The partition key *is* the file path. Re-running a partition overwrites exactly
one file, which is what makes collection idempotent.
"""

from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
from typing import Any, Iterable


def data_root() -> Path:
    """Root of the dataset tree. Overridable so tests never touch real data."""
    return Path(os.environ.get("DATASETS_DATA_ROOT", "data"))


def raw_path(source: str, dt: str, part: str | None = None) -> Path:
    """Path for a partition's raw payload.

    A merged source reads the same partition many times a day, so each read
    gets its own file -- overwriting would leave raw able to reconstruct only
    the final read, which is most of the point of keeping it.
    """
    base = data_root() / "raw" / source
    if part:
        return base / f"dt={dt}" / f"{part}.json.gz"
    return base / f"dt={dt}.json.gz"


def curated_path(source: str, dt: str) -> Path:
    return data_root() / "curated" / source / f"dt={dt}.jsonl"


def manifest_path() -> Path:
    return data_root() / "_manifest.jsonl"


def write_raw(source: str, dt: str, payload: Any, part: str | None = None) -> int:
    """Store the upstream response verbatim. Returns bytes written.

    Kept gzipped: a new file lands every day so diffs carry no information, and
    compression is roughly a 6x saving on a repo that grows forever.
    """
    path = raw_path(source, dt, part)
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.open(path, "wb", compresslevel=6) as fh:
        fh.write(blob)
    return path.stat().st_size


def write_curated(source: str, dt: str, records: Iterable[dict[str, Any]]) -> tuple[int, int]:
    """Write normalized records as JSONL. Returns (row count, bytes written)."""
    path = curated_path(source, dt)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            fh.write("\n")
            rows += 1
    return rows, path.stat().st_size


def read_curated(source: str, dt: str) -> list[dict[str, Any]]:
    path = curated_path(source, dt)
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def read_raw(source: str, dt: str) -> Any:
    with gzip.open(raw_path(source, dt), "rb") as fh:
        return json.loads(fh.read().decode("utf-8"))
