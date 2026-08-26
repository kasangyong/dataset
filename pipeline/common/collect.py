"""Shared collection routine.

Every source runs the same sequence: fetch, store raw, normalize, validate,
store curated, record the run. Keeping it here means a new source is a fetch
function plus a normalize function -- nothing else.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from pydantic import ValidationError

from pipeline.common import manifest, storage
from pipeline.common.schema import Record

# Above this share of unparseable records we treat the run as failed rather
# than publishing a thin file: it means the upstream schema moved, and a
# quietly half-empty partition is worse than a loud failure.
MAX_INVALID_SHARE = 0.5

FetchFn = Callable[[str], Any]
NormalizeFn = Callable[[Any, str], list[dict[str, Any]]]


class SchemaDriftError(RuntimeError):
    """Too many records failed validation to trust the payload."""


class EmptyPayloadError(RuntimeError):
    """The source returned no records at all."""


def collect(
    context: AssetExecutionContext,
    *,
    source: str,
    fetch: FetchFn,
    normalize: NormalizeFn,
    model: type[Record],
    allow_empty: bool = False,
) -> MaterializeResult:
    dt = context.partition_key
    started = time.monotonic()

    try:
        payload = fetch(dt)
        # Stamped here rather than after normalization: this is the instant the
        # counts in the payload were true.
        collected_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        bytes_raw = storage.write_raw(source, dt, payload)

        candidates = normalize(payload, dt)
        if not candidates and not allow_empty:
            raise EmptyPayloadError(f"{source} returned no records for {dt}")

        valid: list[dict[str, Any]] = []
        invalid = 0
        for candidate in candidates:
            try:
                stamped = {**candidate, "collected_at": collected_at}
                valid.append(model.model_validate(stamped).model_dump())
            except ValidationError as exc:
                invalid += 1
                if invalid <= 3:  # log a few examples, not thousands
                    context.log.warning(f"{source} {dt}: invalid record: {exc}")

        if candidates and invalid / len(candidates) > MAX_INVALID_SHARE:
            raise SchemaDriftError(
                f"{source} {dt}: {invalid}/{len(candidates)} records failed validation"
            )

        rows, bytes_curated = storage.write_curated(source, dt, valid)
        duration = time.monotonic() - started

        entry = manifest.append(
            source=source,
            dt=dt,
            status="ok",
            rows=rows,
            invalid_rows=invalid,
            bytes_raw=bytes_raw,
            bytes_curated=bytes_curated,
            duration_s=duration,
        )
        context.log.info(f"{source} {dt}: {rows} rows ({invalid} invalid)")

    except Exception as exc:
        duration = time.monotonic() - started
        manifest.append(
            source=source,
            dt=dt,
            status="failed",
            duration_s=duration,
            error=f"{type(exc).__name__}: {exc}"[:500],
        )
        raise

    return MaterializeResult(
        metadata={
            "rows": entry["rows"],
            "invalid_rows": entry["invalid_rows"],
            "bytes_curated": entry["bytes_curated"],
            "duration_s": entry["duration_s"],
            "path": MetadataValue.path(str(storage.curated_path(source, dt))),
        }
    )
