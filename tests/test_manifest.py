"""Run history survives CI, where Dagster's own instance DB does not."""

from pipeline.common import manifest
from tests.conftest import DT


def test_success_entry_records_counts():
    entry = manifest.append(
        source="fx_rates", dt=DT, status="ok", rows=29, invalid_rows=1,
        bytes_raw=500, bytes_curated=900, duration_s=1.2345,
    )

    assert entry["status"] == "ok"
    assert entry["rows"] == 29
    assert entry["duration_s"] == 1.234  # rounded for readability
    assert entry["error"] is None
    assert entry["run_at"].endswith("Z")


def test_failure_entry_keeps_the_error():
    manifest.append(source="fx_rates", dt=DT, status="failed", error="FetchError: 503")

    entry = manifest.read_all()[-1]
    assert entry["status"] == "failed"
    assert entry["rows"] == 0
    assert "503" in entry["error"]


def test_entries_accumulate_rather_than_overwrite():
    manifest.append(source="fx_rates", dt=DT, status="failed", error="boom")
    manifest.append(source="fx_rates", dt=DT, status="ok", rows=29)

    assert len(manifest.read_all()) == 2


def test_latest_status_takes_the_last_line_per_source():
    manifest.append(source="fx_rates", dt=DT, status="failed", error="boom")
    manifest.append(source="fx_rates", dt=DT, status="ok", rows=29)
    manifest.append(source="hn_stories", dt=DT, status="failed", error="boom")

    assert manifest.latest_status(DT) == {"fx_rates": "ok", "hn_stories": "failed"}


def test_latest_status_ignores_other_partitions():
    manifest.append(source="fx_rates", dt="2026-08-24", status="ok", rows=29)

    assert manifest.latest_status(DT) == {}


def test_reading_before_any_run_is_empty():
    assert manifest.read_all() == []
