"""Scheduler driver: date arithmetic, source isolation, exit codes."""

import pytest

from pipeline import cli


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


def test_date_range_of_one_day():
    assert cli.date_range("2026-08-24", "2026-08-24") == ["2026-08-24"]


def test_reversed_date_range_is_rejected():
    with pytest.raises(ValueError):
        cli.date_range("2026-08-26", "2026-08-24")


def test_all_sources_run_for_every_date(fake_materialize):
    calls = fake_materialize({})

    assert cli.run(["2026-08-24", "2026-08-25"], list(cli.BY_NAME)) == 0
    assert len(calls) == 2 * len(cli.BY_NAME)


def test_one_failing_source_does_not_stop_the_others(fake_materialize):
    calls = fake_materialize({"fx_rates": False})

    code = cli.run(["2026-08-25"], list(cli.BY_NAME))

    assert code == 0  # partial data is still worth committing
    assert len(calls) == len(cli.BY_NAME)


def test_a_day_with_every_source_failing_fails_the_run(fake_materialize):
    fake_materialize({name: False for name in cli.BY_NAME})

    assert cli.run(["2026-08-25"], list(cli.BY_NAME)) == 1


def test_one_dead_day_in_a_backfill_fails_the_run(fake_materialize):
    calls = fake_materialize({name: False for name in cli.BY_NAME})

    code = cli.run(["2026-08-24", "2026-08-25"], list(cli.BY_NAME))

    assert code == 1
    assert len(calls) == 2 * len(cli.BY_NAME)  # every pair still attempted


def test_defaults_to_yesterday(fake_materialize, monkeypatch):
    calls = fake_materialize({})
    monkeypatch.setattr(cli, "yesterday_kst", lambda: "2026-08-25")

    assert cli.main([]) == 0
    assert {dt for _, dt in calls} == {"2026-08-25"}


def test_source_selection_is_respected(fake_materialize):
    calls = fake_materialize({})

    cli.main(["--start", "2026-08-25", "--sources", "fx_rates,hn_stories"])

    assert {name for name, _ in calls} == {"fx_rates", "hn_stories"}


def test_unknown_source_is_rejected_before_running(fake_materialize):
    calls = fake_materialize({})

    with pytest.raises(SystemExit):
        cli.main(["--sources", "nasdaq"])

    assert calls == []


def test_start_without_end_collects_a_single_day(fake_materialize):
    calls = fake_materialize({})

    cli.main(["--start", "2026-08-20"])

    assert {dt for _, dt in calls} == {"2026-08-20"}
