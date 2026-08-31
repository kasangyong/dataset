"""Append-only run history.

Dagster's own run history lives in a SQLite instance that is discarded on every
GitHub Actions run. This file is committed alongside the data, so it survives.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal

from pipeline.common.storage import manifest_path

Status = Literal["ok", "failed"]


def append(
    *,
    source: str,
    dt: str,
    status: Status,
    rows: int = 0,
    invalid_rows: int = 0,
    bytes_raw: int = 0,
    bytes_curated: int = 0,
    duration_s: float = 0.0,
    error: str | None = None,
) -> dict[str, Any]:
    entry = {
        "source": source,
        "dt": dt,
        "status": status,
        "rows": rows,
        "invalid_rows": invalid_rows,
        "bytes_raw": bytes_raw,
        "bytes_curated": bytes_curated,
        "duration_s": round(duration_s, 3),
        "run_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "error": error,
    }
    path = manifest_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False, sort_keys=True))
        fh.write("\n")
    return entry


def read_all() -> list[dict[str, Any]]:
    path = manifest_path()
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def latest_status(dt: str) -> dict[str, str]:
    """Most recent status per source for one partition.

    Entries are append-only, so a re-run leaves several lines for the same
    (source, dt). The latest ``run_at`` wins -- not the last line, because a
    union merge of a local run and a CI run interleaves them in arbitrary
    order, and reading positionally would resurrect a stale failure.
    """
    result: dict[str, str] = {}
    for entry in sorted(read_all(), key=lambda e: e.get("run_at", "")):
        if entry.get("dt") == dt:
            result[entry["source"]] = entry["status"]
    return result
