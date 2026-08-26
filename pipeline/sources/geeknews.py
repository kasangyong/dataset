"""GeekNews posts (news.hada.io Atom feed, no credentials).

Korean developer news. The feed is a rolling window of fifty entries, measured
at about forty hours -- comfortable for a daily read today, but a busy stretch
would push older entries out before it. Reading hourly and merging removes that
dependency on posting volume.

Title, link and the feed's own summary only; the article body is not stored.
"""

import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

from dagster import AssetExecutionContext, Backoff, MaterializeResult, RetryPolicy, asset

from pipeline.common.collect import collect
from pipeline.common.http import get_text
from pipeline.common.partitions import DAILY_OPEN, KST
from pipeline.common.schema import GeekNewsPost

SOURCE = "geeknews"
ENDPOINT = "https://news.hada.io/rss/news"
ATOM = "{http://www.w3.org/2005/Atom}"
# A quiet hour adds nothing new; that is not a failure.
ALLOW_EMPTY = True

TAGS = re.compile(r"<[^>]+>")


def fetch(dt: str) -> Any:
    return get_text(ENDPOINT)


def _plain_text(markup: str | None) -> str | None:
    """The feed's summary is an HTML bullet list; store it as readable text."""
    if not markup:
        return None
    text = TAGS.sub(" ", markup)
    return " ".join(html.unescape(text).split()) or None


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    records = []
    for entry in ET.fromstring(payload.encode("utf-8")).findall(f"{ATOM}entry"):
        stamp = entry.findtext(f"{ATOM}published") or entry.findtext(f"{ATOM}updated")
        published = datetime.fromisoformat(stamp)
        # The window spans more than a day, so entries from other days appear.
        if published.astimezone(KST).date().isoformat() != dt:
            continue

        link = entry.find(f"{ATOM}link")
        url = link.get("href") if link is not None else entry.findtext(f"{ATOM}id")
        records.append(
            {
                "dt": dt,
                "topic_id": (url or "").rsplit("=", 1)[-1],
                "title": (entry.findtext(f"{ATOM}title") or "").strip(),
                "url": url,
                "summary": _plain_text(entry.findtext(f"{ATOM}content")),
                "published": published.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )
    return records


@asset(
    name=SOURCE,
    partitions_def=DAILY_OPEN,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="GeekNews developer posts, collected hourly and merged into the day.",
)
def geeknews(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=GeekNewsPost,
        merge_key="topic_id",
        allow_empty=ALLOW_EMPTY,
    )
