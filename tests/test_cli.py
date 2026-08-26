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
