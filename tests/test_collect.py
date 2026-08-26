"""The shared collection routine: what it publishes, and when it refuses to."""

import pytest
from dagster import build_asset_context

from pipeline.common import manifest, storage
from pipeline.common.collect import EmptyPayloadError, SchemaDriftError, collect
from pipeline.common.http import FetchError
from pipeline.common.schema import FxRate
from tests.conftest import DT

SOURCE = "fx_rates"


def make_payload(n=3):
    return {"date": DT, "base": "USD", "rates": {f"C{i}": 1.0 + i for i in range(n)}}


def make_records(payload, dt):
    return [
        {"dt": dt, "base": "USD", "quote": q, "rate": r, "rate_date": payload["date"],
         "is_stale": payload["date"] != dt}
        for q, r in payload["rates"].items()
    ]


def run(fetch=None, normalize=None, dt=DT, **kwargs):
    return collect(
        build_asset_context(partition_key=dt),
        source=SOURCE,
        fetch=fetch or (lambda d: make_payload()),
        normalize=normalize or make_records,
        model=FxRate,
        **kwargs,
    )


def test_successful_run_writes_raw_curated_and_manifest():
    result = run()

    assert storage.read_raw(SOURCE, DT) == make_payload()
    assert len(storage.read_curated(SOURCE, DT)) == 3
    assert result.metadata["rows"] == 3

    entry = manifest.read_all()[-1]
    assert entry["status"] == "ok"
    assert entry["rows"] == 3
    assert entry["invalid_rows"] == 0
    assert entry["bytes_raw"] > 0


def test_rerunning_a_partition_does_not_duplicate_rows():
    run()
    run()

    assert len(storage.read_curated(SOURCE, DT)) == 3
    # ...but both attempts are visible in the history
    assert len(manifest.read_all()) == 2


def test_fetch_failure_is_recorded_then_reraised():
    def boom(dt):
        raise FetchError("503 from upstream")

    with pytest.raises(FetchError):
        run(fetch=boom)

    entry = manifest.read_all()[-1]
    assert entry["status"] == "failed"
    assert "503" in entry["error"]


def test_failure_leaves_no_curated_file_behind():
    def boom(dt):
        raise FetchError("down")

    with pytest.raises(FetchError):
        run(fetch=boom)

    assert not storage.curated_path(SOURCE, DT).exists()


def test_empty_payload_fails_loudly():
    with pytest.raises(EmptyPayloadError):
        run(normalize=lambda payload, dt: [])

    assert manifest.read_all()[-1]["status"] == "failed"


def test_empty_payload_is_allowed_when_declared():
    result = run(normalize=lambda payload, dt: [], allow_empty=True)

    assert result.metadata["rows"] == 0
    assert manifest.read_all()[-1]["status"] == "ok"


def test_a_few_bad_records_are_dropped_and_counted():
    def with_one_bad(payload, dt):
        records = make_records(payload, dt)
        records[0]["rate"] = "not-a-number"
        return records

    result = run(normalize=with_one_bad)

    assert result.metadata["rows"] == 2
    assert result.metadata["invalid_rows"] == 1
    assert manifest.read_all()[-1]["invalid_rows"] == 1


def test_mostly_bad_records_fail_the_run():
    # Upstream changed its schema; a thin partition would hide that.
    def mostly_bad(payload, dt):
        records = make_records(payload, dt)
        for record in records[:2]:
            record["rate"] = "not-a-number"
        return records

    with pytest.raises(SchemaDriftError):
        run(normalize=mostly_bad)

    assert manifest.read_all()[-1]["status"] == "failed"
    assert not storage.curated_path(SOURCE, DT).exists()


def test_raw_is_kept_even_when_normalization_fails():
    # Raw is the only way to recover a day for APIs that do not serve history.
    def bad(payload, dt):
        raise ValueError("normalizer bug")

    with pytest.raises(ValueError):
        run(normalize=bad)

    assert storage.read_raw(SOURCE, DT) == make_payload()


def test_records_carry_the_partition_they_were_collected_for():
    run(dt="2026-08-24")

    rows = storage.read_curated(SOURCE, "2026-08-24")
    assert all(row["dt"] == "2026-08-24" for row in rows)


def test_every_stored_record_carries_its_observation_time():
    run()

    rows = storage.read_curated(SOURCE, DT)
    assert all(row["collected_at"].endswith("Z") for row in rows)
    # One read of the source is one observation, so the whole batch shares it.
    assert len({row["collected_at"] for row in rows}) == 1


def test_observation_time_is_the_read_not_the_partition():
    # A backfilled day is measured today, not on the day it labels. Conflating
    # the two is what makes stars and points incomparable across partitions.
    run(dt="2026-08-01")

    row = storage.read_curated(SOURCE, "2026-08-01")[0]
    assert row["dt"] == "2026-08-01"
    assert row["collected_at"] > "2026-08-01T00:00:00Z"


def test_normalizers_do_not_supply_the_stamp():
    # If a normalizer set it, every new source would have to remember to.
    captured = {}

    def capture(payload, dt):
        records = make_records(payload, dt)
        captured["keys"] = set(records[0])
        return records

    run(normalize=capture)

    assert "collected_at" not in captured["keys"]
    assert "collected_at" in storage.read_curated(SOURCE, DT)[0]
