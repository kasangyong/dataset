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
