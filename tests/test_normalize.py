"""Normalizers must turn real API payloads into records the schema accepts."""

import pytest
from pydantic import ValidationError

from pipeline.common.schema import (
    ArxivPaper,
    ClinicalTrial,
    CryptoMarket,
    FxRate,
    GithubRepo,
    HnStory,
    YnaNews,
)
from pipeline.sources import (
    arxiv,
    clinical_trials,
    crypto,
    fx,
    github_repos,
    hackernews,
    yna_news,
)
from tests.conftest import DT

# Normalizers do not stamp observation time -- the collector does. Tests that
# validate normalizer output supply it the way the collector would.
STAMP = "2026-08-26T03:04:44Z"

# yna_news is absent on purpose: it filters by partition date, so it is
# exercised against its own captured day further down.
CASES = [
    (fx, FxRate, "fx_rates"),
    (github_repos, GithubRepo, "github_repos"),
    (hackernews, HnStory, "hn_stories"),
    (arxiv, ArxivPaper, "arxiv_papers"),
    (crypto, CryptoMarket, "crypto_markets"),
    (clinical_trials, ClinicalTrial, "clinical_trials"),
]


@pytest.mark.parametrize("module, model, fixture_name", CASES)
def test_normalize_produces_valid_records(module, model, fixture_name, load_fixture):
    records = module.normalize(load_fixture(fixture_name), DT)

    assert records, "fixture should yield at least one record"
    for record in records:
        model.model_validate({**record, "collected_at": STAMP})  # raises on drift
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
    HnStory.model_validate({**records[0], "collected_at": STAMP})


def test_hn_stores_no_article_body(load_fixture):
    records = hackernews.normalize(load_fixture("hn_stories"), DT)
    assert "story_text" not in records[0]
    assert set(records[0]) == set(HnStory.model_fields) - {"collected_at"}


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
                "collected_at": STAMP,
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
                "collected_at": STAMP,
            }
        )


def test_github_query_uses_kst_day_bounds(monkeypatch):
    # A bare date would be read as a UTC day and would not line up with the
    # KST partition label the other sources use.
    captured = {}

    def fake_get_json(url, params=None, headers=None, timeout=30):
        captured.update(params)
        return {"items": []}

    monkeypatch.setattr(github_repos, "get_json", fake_get_json)
    github_repos.fetch("2026-08-25")

    assert captured["q"] == "created:2026-08-25T00:00:00+09:00..2026-08-25T23:59:59+09:00"


def test_hn_query_uses_kst_day_bounds(monkeypatch):
    captured = {}

    def fake_get_json(url, params=None, headers=None, timeout=30):
        captured.update(params)
        return {"hits": []}

    monkeypatch.setattr(hackernews, "get_json", fake_get_json)
    hackernews.fetch("2026-08-25")

    # KST midnight on 2026-08-25 is 2026-08-24T15:00:00Z
    from datetime import datetime, timezone

    start = int(captured["numericFilters"].split(",")[0].split(">=")[1])
    assert datetime.fromtimestamp(start, timezone.utc).isoformat() == "2026-08-24T15:00:00+00:00"


def test_arxiv_keeps_abstracts_and_flattens_whitespace(load_fixture):
    records = arxiv.normalize(load_fixture("arxiv_papers"), DT)

    assert records
    paper = records[0]
    assert len(paper["abstract"]) > 100
    # Atom wraps text at column width; unwrapped text is what is usable.
    assert "\n" not in paper["abstract"] and "  " not in paper["abstract"]
    assert paper["arxiv_id"] and "/" not in paper["arxiv_id"]
    assert paper["authors"] and all(paper["authors"])
    assert paper["primary_category"] in paper["categories"]


def test_crypto_records_the_sources_own_timestamp(load_fixture):
    # Distinct from collected_at: one is when the quote was priced, the other
    # when we read it.
    records = crypto.normalize(load_fixture("crypto_markets"), DT)

    assert records[0]["last_updated"]
    assert records[0]["price_usd"] > 0
    assert records[0]["market_cap_rank"] == 1


def test_yna_normalizes_its_captured_day(load_fixture, fixture_meta):
    dt = fixture_meta("yna_news")["dt"]

    records = yna_news.normalize(load_fixture("yna_news"), dt)

    assert records
    for record in records:
        YnaNews.model_validate({**record, "collected_at": STAMP})
        assert record["dt"] == dt
        assert record["published"].endswith("Z")


def test_yna_drops_items_from_other_days(load_fixture, fixture_meta):
    # The feed still lists yesterday's items just after midnight.
    dt = fixture_meta("yna_news")["dt"]
    other_day = "2020-01-01"

    assert yna_news.normalize(load_fixture("yna_news"), other_day) == []
    assert yna_news.normalize(load_fixture("yna_news"), dt) != []


def test_yna_stores_no_article_body(load_fixture, fixture_meta):
    dt = fixture_meta("yna_news")["dt"]
    records = yna_news.normalize(load_fixture("yna_news"), dt)

    assert set(records[0]) == set(YnaNews.model_fields) - {"collected_at"}
    # The feed's description is a lede, not the article.
    assert len(records[0]["summary"] or "") < 500


def test_clinical_trials_flattens_the_registry_modules(load_fixture):
    records = clinical_trials.normalize(load_fixture("clinical_trials"), DT)

    assert records
    trial = records[0]
    assert trial["nct_id"].startswith("NCT")
    assert trial["url"].endswith(trial["nct_id"])
    assert trial["status"]
    # Design, endpoints and eligibility live in separate modules upstream; the
    # point of the normalizer is that one row answers a question.
    assert isinstance(trial["phases"], list)
    assert isinstance(trial["conditions"], list)
    assert isinstance(trial["primary_outcomes"], list)


def test_clinical_trials_reads_every_page(load_fixture):
    # The registry pages by token; a normalizer that saw only the first page
    # would silently drop most of a day.
    payload = load_fixture("clinical_trials")
    one_page = clinical_trials.normalize(payload, DT)

    doubled = {**payload, "pages": payload["pages"] + payload["pages"]}
    assert len(clinical_trials.normalize(doubled, DT)) == 2 * len(one_page)


def test_clinical_trials_stores_registry_metadata_only(load_fixture):
    # Protocol summaries and aggregate design, never patient-level records.
    records = clinical_trials.normalize(load_fixture("clinical_trials"), DT)

    assert set(records[0]) == set(ClinicalTrial.model_fields) - {"collected_at"}
