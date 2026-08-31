"""Scheduler driver: what runs when, source isolation, exit codes."""

import pytest

from pipeline import cli
from pipeline.common.partitions import partition_for
from pipeline.registry import DAILY_SOURCES, HOURLY_SOURCES

ALL = list(cli.BY_NAME)


class FakeResult:
    def __init__(self, success):
        self.success = success


@pytest.fixture
def fake_materialize(monkeypatch):
    """Replace Dagster execution with a scripted outcome per source."""
    calls = []

    def factory(outcomes):
        def _materialize(assets, partition_key, raise_on_error):
            name = assets[0].key.to_user_string()
            calls.append((name, partition_key))
            return FakeResult(outcomes.get(name, True))

        monkeypatch.setattr(cli, "materialize", _materialize)
        return calls

    return factory


def test_date_range_is_inclusive():
    assert cli.date_range("2026-08-24", "2026-08-26") == ["2026-08-24", "2026-08-25", "2026-08-26"]


def test_reversed_date_range_is_rejected():
    with pytest.raises(ValueError):
        cli.date_range("2026-08-26", "2026-08-24")


def test_each_source_defaults_to_its_own_due_partition():
    # A completed-day source lags a day; a live snapshot belongs to today.
    plan = dict((name, dt) for dt, name in cli.build_plan(ALL, None, None))

    assert plan["fx_rates"] == partition_for(1)
    assert plan["crypto_markets"] == partition_for(0)
    assert plan["fx_rates"] != plan["crypto_markets"]


def test_backfill_covers_every_day_for_backfillable_sources():
    plan = cli.build_plan(["fx_rates"], "2026-08-01", "2026-08-03")

    assert [dt for dt, _ in plan] == ["2026-08-01", "2026-08-02", "2026-08-03"]


def test_snapshot_sources_are_not_backfilled():
    # Writing today's prices under a past date would invent data that never was.
    plan = cli.build_plan(["crypto_markets"], "2026-08-01", "2026-08-03")

    assert plan == []


def test_snapshot_source_still_runs_for_its_own_partition():
    today = partition_for(0)
    plan = cli.build_plan(["crypto_markets"], today, today)

    assert plan == [(today, "crypto_markets")]


def test_backfill_mixes_backfillable_and_snapshot_sources_correctly():
    plan = cli.build_plan(["fx_rates", "crypto_markets"], "2026-08-01", "2026-08-02")

    assert sorted(plan) == [("2026-08-01", "fx_rates"), ("2026-08-02", "fx_rates")]


def test_groups_split_by_cadence(fake_materialize):
    calls = fake_materialize({})

    cli.main(["--group", "hourly"])

    assert {name for name, _ in calls} == set(HOURLY_SOURCES)
    assert set(HOURLY_SOURCES).isdisjoint(DAILY_SOURCES)


def test_daily_group_excludes_hourly_sources(fake_materialize):
    calls = fake_materialize({})

    cli.main(["--group", "daily"])

    assert {name for name, _ in calls} == set(DAILY_SOURCES)


def test_explicit_sources_override_the_group(fake_materialize):
    calls = fake_materialize({})

    cli.main(["--group", "daily", "--sources", "yna_news"])

    assert {name for name, _ in calls} == {"yna_news"}


def test_one_failing_source_does_not_stop_the_others(fake_materialize):
    calls = fake_materialize({"fx_rates": False})

    code = cli.run(cli.build_plan(ALL, None, None))

    assert code == 0  # partial data is still worth committing
    assert len(calls) == len(ALL)


def test_a_day_with_every_source_failing_fails_the_run(fake_materialize):
    fake_materialize({name: False for name in ALL})

    assert cli.run(cli.build_plan(["fx_rates", "hn_stories"], None, None)) == 1


def test_one_dead_day_in_a_backfill_fails_the_run(fake_materialize):
    calls = fake_materialize({"fx_rates": False})

    code = cli.run(cli.build_plan(["fx_rates"], "2026-08-01", "2026-08-02"))

    assert code == 1
    assert len(calls) == 2  # every pair still attempted


def test_unknown_source_is_rejected_before_running(fake_materialize):
    calls = fake_materialize({})

    with pytest.raises(SystemExit):
        cli.main(["--sources", "nasdaq"])

    assert calls == []


# --- repairing partitions that already failed ------------------------------


from datetime import date, timedelta

from pipeline.common import manifest, storage


def land(name, dt, rows=1, status="ok"):
    """Record an outcome the way a real run would."""
    manifest.append(source=name, dt=dt, status=status, rows=rows)
    if status == "ok":
        storage.write_curated(name, dt, [{"dt": dt}] * rows)


def due_minus(name, days):
    return (date.fromisoformat(partition_for(cli.BY_NAME[name].lag_days)) - timedelta(days=days)).isoformat()


def test_a_partition_that_never_landed_is_a_gap():
    assert (due_minus("fx_rates", 1), "fx_rates") in cli.find_gaps(["fx_rates"])


def test_a_partition_that_failed_is_a_gap():
    dt = due_minus("fx_rates", 1)
    land("fx_rates", dt, status="failed")

    assert (dt, "fx_rates") in cli.find_gaps(["fx_rates"])


def test_a_healthy_partition_is_not_a_gap():
    dt = due_minus("fx_rates", 1)
    land("fx_rates", dt, rows=29)

    assert (dt, "fx_rates") not in cli.find_gaps(["fx_rates"])


def test_a_legitimately_empty_partition_is_not_a_gap():
    # Weekends really do have no clinical trials; an empty file is the answer,
    # not a hole to keep re-drilling.
    dt = due_minus("clinical_trials", 1)
    manifest.append(source="clinical_trials", dt=dt, status="ok", rows=0)
    storage.write_curated("clinical_trials", dt, [])

    assert (dt, "clinical_trials") not in cli.find_gaps(["clinical_trials"])


def test_a_success_whose_file_vanished_is_a_gap():
    dt = due_minus("fx_rates", 1)
    manifest.append(source="fx_rates", dt=dt, status="ok", rows=29)  # no file written

    assert (dt, "fx_rates") in cli.find_gaps(["fx_rates"])


def test_snapshot_sources_are_never_repaired():
    # Re-reading a live snapshot would file today's numbers under a past date.
    assert cli.find_gaps(["crypto_markets", "yna_news", "geeknews"]) == []


def test_the_due_partition_is_left_to_the_normal_plan():
    due = partition_for(cli.BY_NAME["fx_rates"].lag_days)

    assert due not in [dt for dt, _ in cli.find_gaps(["fx_rates"])]


def test_repair_reaches_back_a_bounded_number_of_days():
    gaps = [dt for dt, _ in cli.find_gaps(["fx_rates"])]

    assert len(gaps) == cli.HEAL_DAYS
    assert min(gaps) == due_minus("fx_rates", cli.HEAL_DAYS)


def test_repair_does_not_reach_before_the_first_partition():
    assert all(dt >= "2026-08-01" for dt, _ in cli.find_gaps(["fx_rates"], days=400))


def test_a_failed_repair_does_not_fail_the_run(fake_materialize):
    # An upstream that is down would otherwise paint every run red until it
    # comes back.
    fake_materialize({"fx_rates": False})
    healing = [(due_minus("fx_rates", 2), "fx_rates")]

    assert cli.run([], healing) == 0


def test_repairs_are_attempted_alongside_the_due_partitions(fake_materialize):
    calls = fake_materialize({})
    healing = [(due_minus("fx_rates", 2), "fx_rates")]

    cli.run(cli.build_plan(["fx_rates"], None, None), healing)

    assert len(calls) == 2


def test_an_explicit_range_does_not_trigger_repair(fake_materialize):
    calls = fake_materialize({})

    cli.main(["--sources", "fx_rates", "--start", "2026-08-10"])

    assert [dt for _, dt in calls] == ["2026-08-10"]


def test_repair_can_be_switched_off(fake_materialize):
    calls = fake_materialize({})

    cli.main(["--sources", "fx_rates", "--no-heal"])

    assert [dt for _, dt in calls] == [partition_for(cli.BY_NAME["fx_rates"].lag_days)]
