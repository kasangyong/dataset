"""The catalog must stay honest: it drives the driver and the docs."""

import pathlib

from pipeline.registry import ASSETS, BY_NAME, DAILY_SOURCES, HOURLY_SOURCES, SOURCES

README = pathlib.Path(__file__).parent.parent / "README.md"


def test_names_are_unique():
    assert len(BY_NAME) == len(SOURCES)


def test_every_entry_maps_to_its_own_asset():
    for source in SOURCES:
        assert source.asset.key.to_user_string() == source.name


def test_assets_and_sources_stay_in_step():
    assert len(ASSETS) == len(SOURCES)


def test_cadence_groups_partition_the_catalog():
    assert sorted(DAILY_SOURCES + HOURLY_SOURCES) == sorted(BY_NAME)


def test_readme_documents_every_dataset():
    # The catalog is the thing people read first; drift here is silent.
    text = README.read_text(encoding="utf-8")
    for source in SOURCES:
        assert source.title in text, f"README is missing the title for {source.name}"
        assert f"`{source.name}`" in text, f"README is missing the id {source.name}"


def test_snapshot_sources_are_marked_unbackfillable():
    # A source with no completed-day form cannot be filled in after the fact.
    for source in SOURCES:
        if source.lag_days == 0:
            assert not source.backfillable, f"{source.name} would invent past data"


def test_hourly_sources_merge_rather_than_overwrite():
    # Reading a partition many times a day and overwriting would keep only the
    # final read.
    for name in HOURLY_SOURCES:
        assert BY_NAME[name].merge_key, f"{name} would discard earlier reads"


def test_clinical_trials_waits_for_the_registry_day_to_close():
    # StudyFirstPostDate is a US Eastern date; at 06:00 KST the previous KST
    # day is still mid-afternoon there.
    assert BY_NAME["clinical_trials"].lag_days == 2


def test_sources_with_legitimately_empty_days_say_so():
    # An empty day must be either a declared possibility or a failure -- never
    # a quiet zero. The registry posts nothing at weekends, and a feed can find
    # nothing new in a quiet hour.
    from pipeline.sources import clinical_trials, fx, yna_news

    assert clinical_trials.ALLOW_EMPTY is True
    assert yna_news.ALLOW_EMPTY is True
    # A source that always has data must not declare it.
    assert not hasattr(fx, "ALLOW_EMPTY")
