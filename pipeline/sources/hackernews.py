"""Top Hacker News stories for the partition date (Algolia search API).

Metadata and link only -- article bodies are deliberately not stored, which
keeps the dataset clear of third-party content licensing.
"""

from typing import Any

from dagster import AssetExecutionContext, Backoff, MaterializeResult, RetryPolicy, asset

from pipeline.common.collect import collect
from pipeline.common.http import get_json
from pipeline.common.partitions import DAILY, day_bounds_epoch
from pipeline.common.schema import HnStory

SOURCE = "hn_stories"
ENDPOINT = "https://hn.algolia.com/api/v1/search"
HITS = 100


def fetch(dt: str) -> Any:
    start, end = day_bounds_epoch(dt)
    return get_json(
        ENDPOINT,
        params={
            "tags": "story",
            "numericFilters": f"created_at_i>={start},created_at_i<{end}",
            "hitsPerPage": HITS,
        },
    )


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    return [
        {
            "dt": dt,
            "object_id": hit.get("objectID"),
            "title": hit.get("title"),
            # Ask HN / Show HN text posts carry no outbound URL.
            "url": hit.get("url"),
            "author": hit.get("author"),
            "points": hit.get("points") or 0,
            "num_comments": hit.get("num_comments") or 0,
            "created_at": hit.get("created_at"),
        }
        for hit in payload.get("hits", [])
    ]


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Top 100 Hacker News stories posted on the partition date.",
)
def hn_stories(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=HnStory,
    )
