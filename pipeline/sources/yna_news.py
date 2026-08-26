"""Yonhap News headlines (RSS, no credentials).

The feed holds roughly the last ninety minutes, so a once-a-day read would
capture about two percent of a day. It is collected hourly and merged into the
day's partition instead.

Headline, lede and link only. The article body is not stored.
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from dagster import AssetExecutionContext, Backoff, MaterializeResult, RetryPolicy, asset

from pipeline.common.collect import collect
from pipeline.common.http import get_text
from pipeline.common.partitions import KST, DAILY_OPEN
from pipeline.common.schema import YnaNews

SOURCE = "yna_news"
ENDPOINT = "https://www.yna.co.kr/rss/news.xml"
DC = "{http://purl.org/dc/elements/1.1/}"
# An hourly read can legitimately find nothing new in a quiet hour.
ALLOW_EMPTY = True


def fetch(dt: str) -> Any:
    return get_text(ENDPOINT)


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    records = []
    for item in ET.fromstring(payload.encode("utf-8")).findall(".//item"):
        published = parsedate_to_datetime(item.findtext("pubDate"))
        # Near midnight the feed still lists the previous day; those items
        # belong to that day's partition, not this one.
        if published.astimezone(KST).date().isoformat() != dt:
            continue
        records.append(
            {
                "dt": dt,
                "guid": item.findtext("guid") or item.findtext("link"),
                "title": (item.findtext("title") or "").strip(),
                "summary": (item.findtext("description") or "").strip() or None,
                "link": item.findtext("link"),
                "author": item.findtext(f"{DC}creator"),
                "published": published.astimezone(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
            }
        )
    return records


@asset(
    name=SOURCE,
    partitions_def=DAILY_OPEN,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Yonhap News headlines, collected hourly and merged into the day.",
)
def yna_news(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=YnaNews,
        merge_key="guid",
        allow_empty=ALLOW_EMPTY,
    )
