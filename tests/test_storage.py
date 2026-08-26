"""Path conventions and idempotency: the partition key is the file path."""

from pipeline.common import storage
from tests.conftest import DT


def test_paths_encode_source_and_partition(isolated_data_root):
    assert storage.raw_path("fx_rates", DT) == isolated_data_root / "raw" / "fx_rates" / f"dt={DT}.json.gz"
    assert storage.curated_path("fx_rates", DT) == isolated_data_root / "curated" / "fx_rates" / f"dt={DT}.jsonl"
    assert storage.manifest_path() == isolated_data_root / "_manifest.jsonl"


def test_raw_survives_a_roundtrip():
    payload = {"date": DT, "rates": {"EUR": 0.9, "한국": 1300.0}}
    written = storage.write_raw("fx_rates", DT, payload)

    assert written > 0
    assert storage.read_raw("fx_rates", DT) == payload


def test_raw_is_actually_compressed():
    # A repo that grows forever pays for this every day, so assert it.
    payload = {"items": [{"description": "x" * 200} for _ in range(50)]}
    written = storage.write_raw("github_repos", DT, payload)

    assert written < 2000


def test_curated_writes_one_json_object_per_line():
    records = [{"dt": DT, "n": 1}, {"dt": DT, "n": 2}]
    rows, size = storage.write_curated("fx_rates", DT, records)

    assert rows == 2
    assert size > 0
    assert storage.read_curated("fx_rates", DT) == records


def test_curated_keys_are_sorted_for_stable_diffs():
    storage.write_curated("fx_rates", DT, [{"z": 1, "a": 2, "dt": DT}])
    line = storage.curated_path("fx_rates", DT).read_text(encoding="utf-8").strip()

    assert line == '{"a": 2, "dt": "%s", "z": 1}' % DT


def test_rewriting_a_partition_replaces_it():
    storage.write_curated("fx_rates", DT, [{"dt": DT, "n": i} for i in range(5)])
    rows, _ = storage.write_curated("fx_rates", DT, [{"dt": DT, "n": i} for i in range(5)])

    assert rows == 5
    assert len(storage.read_curated("fx_rates", DT)) == 5


def test_partitions_do_not_collide():
    storage.write_curated("fx_rates", "2026-08-25", [{"dt": "2026-08-25"}])
    storage.write_curated("fx_rates", "2026-08-26", [{"dt": "2026-08-26"}])

    assert storage.read_curated("fx_rates", "2026-08-25") == [{"dt": "2026-08-25"}]
    assert storage.read_curated("fx_rates", "2026-08-26") == [{"dt": "2026-08-26"}]


def test_reading_a_missing_partition_is_empty_not_an_error():
    assert storage.read_curated("fx_rates", "1999-01-01") == []


def test_kst_day_bounds_cover_exactly_one_day():
    from pipeline.common.partitions import day_bounds_epoch, day_bounds_iso

    start, end = day_bounds_epoch("2026-08-25")
    assert end - start == 24 * 3600
    assert day_bounds_iso("2026-08-25") == (
        "2026-08-25T00:00:00+09:00",
        "2026-08-25T23:59:59+09:00",
    )
