"""Normalizers must turn real API payloads into records the schema accepts."""

import pytest
from pydantic import ValidationError

from pipeline.common.schema import FxRate, GithubRepo, HnStory
from pipeline.sources import fx, github_repos, hackernews
from tests.conftest import DT

CASES = [
    (fx, FxRate, "fx_rates"),
    (github_repos, GithubRepo, "github_repos"),
    (hackernews, HnStory, "hn_stories"),
]


@pytest.mark.parametrize("module, model, fixture_name", CASES)
def test_normalize_produces_valid_records(module, model, fixture_name, load_fixture):
    records = module.normalize(load_fixture(fixture_name), DT)

    assert records, "fixture should yield at least one record"
    for record in records:
        model.model_validate(record)  # raises on drift
        assert record["dt"] == DT


@pytest.mark.parametrize("module, model, fixture_name", CASES)
def test_normalize_is_pure(module, model, fixture_name, load_fixture):
    payload = load_fixture(fixture_name)
    assert module.normalize(payload, DT) == module.normalize(payload, DT)


def test_fx_marks_carried_forward_rates(load_fixture):
    payload = load_fixture("fx_rates")
    payload["date"] = "2026-08-21"  # ECB published Friday; we asked for Sunday

    records = fx.normalize(payload, "2026-08-23")

    assert records
    assert all(r["is_stale"] for r in records)
    assert all(r["rate_date"] == "2026-08-21" for r in records)


def test_fx_fresh_rates_are_not_stale(load_fixture):
    records = fx.normalize(load_fixture("fx_rates"), DT)
    assert all(not r["is_stale"] for r in records)


def test_hn_keeps_text_posts_without_url(load_fixture):
    payload = load_fixture("hn_stories")
    payload["hits"][0]["url"] = None

    records = hackernews.normalize(payload, DT)

    assert records[0]["url"] is None
    HnStory.model_validate(records[0])


def test_hn_stores_no_article_body(load_fixture):
    records = hackernews.normalize(load_fixture("hn_stories"), DT)
    assert "story_text" not in records[0]
    assert set(records[0]) == set(HnStory.model_fields)


def test_schema_rejects_unknown_field():
    with pytest.raises(ValidationError):
        HnStory.model_validate(
            {
                "dt": DT,
                "object_id": "1",
                "title": "t",
                "url": None,
                "author": "a",
                "points": 1,
                "num_comments": 0,
                "created_at": "2026-08-25T00:00:00Z",
                "surprise": "new upstream field",
            }
        )


def test_schema_rejects_wrong_type():
    with pytest.raises(ValidationError):
        FxRate.model_validate(
            {
                "dt": DT,
                "base": "USD",
                "quote": "EUR",
                "rate": "not-a-number",
                "rate_date": DT,
                "is_stale": False,
            }
        )
