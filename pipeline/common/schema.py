"""Record models. One model per source; all share a partition date."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class Record(BaseModel):
    """Base for every curated record.

    ``extra="forbid"`` is deliberate: if an upstream API grows a field we care
    about, we want the normalizer updated explicitly rather than silently
    passing unvalidated data through.
    """

    model_config = ConfigDict(extra="forbid")

    dt: str  # partition date, YYYY-MM-DD

    # When the source was actually read, in UTC. Counts like stars and points
    # are measured once, at this moment -- so without it a value is not
    # interpretable: 360 points after 6 hours and after 5 days are different
    # facts. Stamped by the collector, not by normalizers.
    collected_at: str


class FxRate(Record):
    base: str
    quote: str
    rate: float
    rate_date: str  # date the API actually returned
    is_stale: bool  # True when rate_date != dt (weekend/holiday carry-forward)


class GithubRepo(Record):
    full_name: str
    owner: str
    language: str | None
    stars: int
    forks: int
    description: str | None
    topics: list[str]
    created_at: str
    html_url: str


class HnStory(Record):
    object_id: str
    title: str
    url: str | None
    author: str | None
    points: int
    num_comments: int
    created_at: str


class ArxivPaper(Record):
    arxiv_id: str
    title: str
    abstract: str
    authors: list[str]
    primary_category: str
    categories: list[str]
    published: str
    updated: str
    pdf_url: str | None


class CryptoMarket(Record):
    coin_id: str
    symbol: str
    name: str
    price_usd: float
    market_cap: int | None
    market_cap_rank: int | None
    volume_24h: float | None
    high_24h: float | None
    low_24h: float | None
    change_pct_24h: float | None
    last_updated: str | None


class YnaNews(Record):
    guid: str
    title: str
    summary: str | None  # the feed's lede, not the article body
    link: str
    author: str | None
    published: str


class ClinicalTrial(Record):
    nct_id: str
    title: str
    status: str
    study_type: str | None
    phases: list[str]
    enrollment: int | None
    enrollment_type: str | None  # ACTUAL once the trial has run, ESTIMATED before
    lead_sponsor: str | None
    sponsor_class: str | None
    conditions: list[str]
    interventions: list[str]
    primary_outcomes: list[str]
    brief_summary: str | None
    sex: str | None
    minimum_age: str | None
    maximum_age: str | None
    healthy_volunteers: bool | None
    countries: list[str]
    start_date: str | None
    completion_date: str | None
    first_posted: str | None
    last_update_posted: str | None
    url: str


class WikipediaTop(Record):
    project: str  # ko.wikipedia / en.wikipedia
    rank: int
    article: str
    views: int


class Earthquake(Record):
    event_id: str
    occurred_at: str
    place: str | None
    magnitude: float | None
    magnitude_type: str | None
    depth_km: float | None
    latitude: float
    longitude: float
    tsunami: int | None
    felt_reports: int | None
    significance: int | None
    url: str | None


class CityWeather(Record):
    city: str
    latitude: float
    longitude: float
    temp_max_c: float | None
    temp_min_c: float | None
    temp_mean_c: float | None
    precipitation_mm: float | None
    windspeed_max_kmh: float | None


class GeekNewsPost(Record):
    topic_id: str
    title: str
    url: str
    summary: str | None  # the feed's bullet lede, not the article
    published: str
