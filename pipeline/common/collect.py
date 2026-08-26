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
    merge_key: str | None = None,
) -> MaterializeResult:
    dt = context.partition_key
    started = time.monotonic()

    try:
        payload = fetch(dt)
        # Stamped here rather than after normalization: this is the instant the
        # counts in the payload were true.
        observed = datetime.now(timezone.utc)
        collected_at = observed.strftime("%Y-%m-%dT%H:%M:%SZ")
        # Merged sources read a partition repeatedly, so each read keeps its own
        # raw file. The name goes finer than collected_at so that two reads in
        # the same second cannot overwrite each other.
        part = observed.strftime("%Y%m%dT%H%M%S%f") if merge_key else None
        bytes_raw = storage.write_raw(source, dt, payload, part)

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

        if merge_key:
            valid = _merge(source, dt, valid, merge_key)

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


def _merge(
    source: str, dt: str, fresh: list[dict[str, Any]], key: str
) -> list[dict[str, Any]]:
    """Union a partition with what it already holds; the first sighting wins.

    A feed that only exposes the last couple of hours needs many reads to cover
    one day. Overwriting would keep just the final read; appending blindly
    would duplicate every item the feed still lists.

    An item already stored keeps its original record, so ``collected_at`` marks
    when the item was first seen rather than the last read that still listed it.
    """
    merged = {record[key]: record for record in storage.read_curated(source, dt)}
    for record in fresh:
        merged.setdefault(record[key], record)
    return sorted(merged.values(), key=lambda r: (_event_time(r), r[key]))


# Sources name their own timestamp differently; ordering a merged partition
# chronologically beats ordering it by an opaque id.
TIME_FIELDS = ("created_at", "published")


def _event_time(record: dict[str, Any]) -> str:
    return next((record[f] for f in TIME_FIELDS if record.get(f)), "")
