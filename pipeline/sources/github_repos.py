"""Repositories created on the partition date, ranked by stars (GitHub Search API).

Works unauthenticated at 10 requests/minute; a token raises that to 30. In CI
the workflow's built-in GITHUB_TOKEN is passed through.
"""

import os
from typing import Any

from dagster import AssetExecutionContext, Backoff, MaterializeResult, RetryPolicy, asset

from pipeline.common.collect import collect
from pipeline.common.http import get_json
from pipeline.common.partitions import DAILY, day_bounds_iso
from pipeline.common.schema import GithubRepo

SOURCE = "github_repos"
ENDPOINT = "https://api.github.com/search/repositories"
PER_PAGE = 100


def fetch(dt: str) -> Any:
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    # A bare `created:YYYY-MM-DD` is interpreted as a UTC day, which is not
    # the day the partition label names. Explicit KST bounds keep this source
    # joinable with the others on `dt`.
    start, end = day_bounds_iso(dt)
    return get_json(
        ENDPOINT,
        params={
            "q": f"created:{start}..{end}",
            "sort": "stars",
            "order": "desc",
            "per_page": PER_PAGE,
        },
        headers=headers,
    )


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    return [
        {
            "dt": dt,
            "full_name": item.get("full_name"),
            "owner": (item.get("owner") or {}).get("login"),
            "language": item.get("language"),
            "stars": item.get("stargazers_count"),
            "forks": item.get("forks_count"),
            "description": item.get("description"),
            "topics": item.get("topics") or [],
            "created_at": item.get("created_at"),
            "html_url": item.get("html_url"),
        }
        for item in payload.get("items", [])
    ]


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Up to 100 repositories created on the partition date, ranked by stars.",
)
def github_repos(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=GithubRepo,
    )
