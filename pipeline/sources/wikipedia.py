"""Most-viewed Wikipedia articles for the partition date (Wikimedia REST API).

Both Korean and English are collected. The Korean side is the widest available
read on what people here looked up on a given day.

Unlike the event sources, ``dt`` here means a **UTC** calendar day: the API
aggregates per UTC day and offers no finer endpoint, so there is no KST window
to align to. Against a KST-day source the two differ by nine hours -- close
enough to compare trends, not close enough to join exactly.

The aggregate is published a few hours after its UTC day closes, which is why
the registry gives this source a two-day lag.
"""

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
from pipeline.common.http import get_json
from pipeline.common.partitions import DAILY
from pipeline.common.schema import WikipediaTop

SOURCE = "wikipedia_top"
ENDPOINT = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{project}/all-access/{y}/{m}/{d}"
PROJECTS = ["ko.wikipedia", "en.wikipedia"]
# The API returns close to a thousand titles; past a few hundred the daily
# counts are in the low hundreds and carry little signal for the size they add.
TOP_N = 500

# Navigation rather than subject matter -- the main page alone outdraws every
# real article, which would make any ranking useless.
SKIP_EXACT = {"Main_Page", "위키백과:대문", "-"}
SKIP_PREFIXES = ("특수:", "Special:", "위키백과:", "Wikipedia:", "Portal:", "포털:")


def _is_navigation(title: str) -> bool:
    return title in SKIP_EXACT or title.startswith(SKIP_PREFIXES)


def fetch(dt: str) -> Any:
    y, m, d = dt.split("-")
    return {
        project: get_json(ENDPOINT.format(project=project, y=y, m=m, d=d))
        for project in PROJECTS
    }


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    log = get_dagster_logger()
    records = []

    for project, response in payload.items():
        items = response.get("items") or [{}]
        articles = [a for a in items[0].get("articles", []) if not _is_navigation(a["article"])]
        if len(articles) > TOP_N:
            log.info(f"{dt} {project}: {len(articles)} articles ranked; keeping top {TOP_N}")

        # Ranks are renumbered after dropping navigation so they stay contiguous.
        for rank, article in enumerate(articles[:TOP_N], start=1):
            records.append(
                {
                    "dt": dt,
                    "project": project,
                    "rank": rank,
                    "article": article["article"],
                    "views": article["views"],
                }
            )
    return records


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Top viewed Korean and English Wikipedia articles for the partition date.",
)
def wikipedia_top(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=WikipediaTop,
    )
