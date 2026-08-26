"""Papers submitted to arXiv on the partition date (Atom API, no credentials).

Abstracts are included: arXiv distributes them for exactly this purpose, so
unlike news bodies they carry no licensing question.
"""

import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
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
from pipeline.common.http import get_text
from pipeline.common.partitions import DAILY, day_bounds_epoch
from pipeline.common.schema import ArxivPaper

SOURCE = "arxiv_papers"
ENDPOINT = "http://export.arxiv.org/api/query"
CATEGORIES = ["cs.AI", "cs.LG", "cs.CL"]
PAGE_SIZE = 100
# arXiv asks for no more than one request every three seconds.
PAGE_DELAY_S = 3
# Roughly 350 papers land on a weekday across these categories. The ceiling is
# a runaway guard, not a sampling decision -- hitting it is logged, never quiet.
MAX_RESULTS = 800

ATOM = "{http://www.w3.org/2005/Atom}"
OPENSEARCH = "{http://a9.com/-/spec/opensearch/1.1/}"
ARXIV_NS = "{http://arxiv.org/schemas/atom}"


def _query(dt: str) -> str:
    start, end = day_bounds_epoch(dt)
    fmt = "%Y%m%d%H%M"
    lo = datetime.fromtimestamp(start, timezone.utc).strftime(fmt)
    hi = datetime.fromtimestamp(end - 60, timezone.utc).strftime(fmt)
    cats = " OR ".join(f"cat:{c}" for c in CATEGORIES)
    return f"({cats}) AND submittedDate:[{lo} TO {hi}]"


def fetch(dt: str) -> Any:
    """Page through the day's submissions. Returns the raw Atom bodies."""
    log = get_dagster_logger()
    query = _query(dt)
    pages: list[str] = []
    total = None
    offset = 0

    while offset < MAX_RESULTS:
        if pages:
            time.sleep(PAGE_DELAY_S)
        body = get_text(
            ENDPOINT,
            params={
                "search_query": query,
                "start": offset,
                "max_results": PAGE_SIZE,
                "sortBy": "submittedDate",
                "sortOrder": "ascending",
            },
            timeout=60,
        )
        pages.append(body)
        root = ET.fromstring(body)
        if total is None:
            total = int(root.findtext(f"{OPENSEARCH}totalResults") or 0)
        got = len(root.findall(f"{ATOM}entry"))
        offset += got
        if got < PAGE_SIZE or offset >= (total or 0):
            break

    if total and total > MAX_RESULTS:
        log.warning(f"{dt}: arXiv reported {total} papers; kept the first {offset}")

    return {"query": query, "total": total, "fetched": offset, "pages": pages}


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    records = []
    for body in payload["pages"]:
        for entry in ET.fromstring(body).findall(f"{ATOM}entry"):
            pdf = next(
                (
                    link.get("href")
                    for link in entry.findall(f"{ATOM}link")
                    if link.get("title") == "pdf"
                ),
                None,
            )
            primary = entry.find(f"{ARXIV_NS}primary_category")
            records.append(
                {
                    "dt": dt,
                    # The id is a URL; the bare identifier is what people cite.
                    "arxiv_id": (entry.findtext(f"{ATOM}id") or "").rsplit("/", 1)[-1],
                    "title": " ".join((entry.findtext(f"{ATOM}title") or "").split()),
                    "abstract": " ".join((entry.findtext(f"{ATOM}summary") or "").split()),
                    "authors": [
                        a.findtext(f"{ATOM}name") for a in entry.findall(f"{ATOM}author")
                    ],
                    "primary_category": primary.get("term") if primary is not None else "",
                    "categories": [
                        c.get("term") for c in entry.findall(f"{ATOM}category")
                    ],
                    "published": entry.findtext(f"{ATOM}published") or "",
                    "updated": entry.findtext(f"{ATOM}updated") or "",
                    "pdf_url": pdf,
                }
            )
    return records


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="arXiv cs.AI / cs.LG / cs.CL papers submitted on the partition date.",
)
def arxiv_papers(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=ArxivPaper,
    )
