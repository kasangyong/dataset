"""Claims that fact-checkers have ruled on (Snopes, FactCheck.org).

There is no feed of false articles to collect -- nobody publishes one. What
exists is the review: a claim somebody made, and a checker's verdict on it.

The feeds carry only headline and link. The verdict lives in the article's
ClaimReview markup, which is schema.org structured data published precisely so
machines can read it, so each day's articles are opened to pull it out. Snopes
marks up its rulings; FactCheck.org does not, and those rows carry no rating
rather than a guessed one.
"""

import html
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import date
from email.utils import parsedate_to_datetime
from typing import Any

from dagster import (
    AssetExecutionContext,
    Backoff,
    MaterializeResult,
    RetryPolicy,
    asset,
    get_dagster_logger,
)

from pipeline.common.collect import collect
from pipeline.common.http import FetchError, get_text
from pipeline.common.partitions import DAILY, KST
from pipeline.common.schema import FactCheck

SOURCE = "fact_checks"
FEEDS = {
    "snopes": "https://www.snopes.com/feed/",
    "factcheck.org": "https://www.factcheck.org/feed/",
}
# Between them these publish two or three a day, so a day with none is normal.
ALLOW_EMPTY = True
# Courtesy gap between article fetches. There are only a few per day.
PAGE_DELAY_S = 1

TAGS = re.compile(r"<[^>]+>")
LD_JSON = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S
)


class FeedWindowError(RuntimeError):
    """The requested day is older than anything the feeds still list."""


def _plain_text(markup: str | None) -> str | None:
    if not markup:
        return None
    return " ".join(html.unescape(TAGS.sub(" ", markup)).split()) or None


def _entries(feed_body: str) -> list[ET.Element]:
    return ET.fromstring(feed_body.encode("utf-8")).findall(".//item")


def _published_kst(item: ET.Element) -> date:
    return parsedate_to_datetime(item.findtext("pubDate")).astimezone(KST).date()


def _extract_claim_review(page: str) -> dict[str, Any] | None:
    """Pull the ClaimReview node out of a page's structured data."""
    for block in LD_JSON.findall(page):
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        nodes = data if isinstance(data, list) else [data]
        if isinstance(data, dict):
            nodes = nodes + list(data.get("@graph", []))
        for node in nodes:
            if isinstance(node, dict) and "ClaimReview" in str(node.get("@type", "")):
                return node
    return None


def fetch(dt: str) -> Any:
    log = get_dagster_logger()
    feeds = {name: get_text(url) for name, url in FEEDS.items()}

    # The feeds are a rolling window of about eleven days. Asked for a day
    # older than that, they cannot answer -- and an empty partition would read
    # as "nothing was fact-checked", which is a different claim entirely.
    oldest = min(
        _published_kst(item) for body in feeds.values() for item in _entries(body)
    )
    if date.fromisoformat(dt) < oldest:
        raise FeedWindowError(
            f"{dt} predates the feed window, which reaches back to {oldest.isoformat()}"
        )

    links = [
        item.findtext("link")
        for body in feeds.values()
        for item in _entries(body)
        if _published_kst(item).isoformat() == dt and item.findtext("link")
    ]

    reviews: dict[str, Any] = {}
    for i, link in enumerate(links):
        if i:
            time.sleep(PAGE_DELAY_S)
        try:
            reviews[link] = _extract_claim_review(get_text(link))
        except FetchError as exc:
            # One unreachable article should not cost us the rest of the day.
            log.warning(f"{dt}: could not read {link}: {exc}")
            reviews[link] = None

    return {"feeds": feeds, "claim_reviews": reviews}


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    records = []
    for publisher, body in payload["feeds"].items():
        for item in _entries(body):
            if _published_kst(item).isoformat() != dt:
                continue

            url = item.findtext("link")
            review = (payload.get("claim_reviews") or {}).get(url) or {}
            rating = review.get("reviewRating") or {}
            records.append(
                {
                    "dt": dt,
                    "publisher": publisher,
                    "guid": item.findtext("guid") or url,
                    "title": (item.findtext("title") or "").strip(),
                    "url": url,
                    "author": item.findtext("{http://purl.org/dc/elements/1.1/}creator"),
                    "topics": [c.text for c in item.findall("category") if c.text],
                    "published": parsedate_to_datetime(item.findtext("pubDate"))
                    .astimezone(KST)
                    .isoformat(),
                    "claim_reviewed": _plain_text(review.get("claimReviewed")),
                    # alternateName is the human-readable verdict ("Fake",
                    # "False"); ratingValue is a scale that differs per checker.
                    "rating": rating.get("alternateName"),
                }
            )
    return records


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Claims ruled on by Snopes and FactCheck.org, with the verdict where published.",
)
def fact_checks(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=FactCheck,
        merge_key="guid",
        allow_empty=ALLOW_EMPTY,
    )
