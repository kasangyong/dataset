"""Trials first registered on the partition date (ClinicalTrials.gov API v2).

Registry metadata: protocol design, sponsor, endpoints, eligibility, and the
public summary. Not patient-level data, which no public API exposes.

`StudyFirstPostDate` is a US Eastern calendar date with no time component, so
unlike the feed sources there is no KST boundary to align to -- the partition
label means the registry's own posting date. The registry ranks new studies by
no useful order, so pages are read until the day is exhausted.
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
from pipeline.common.schema import ClinicalTrial

SOURCE = "clinical_trials"
ENDPOINT = "https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE = 200
# Around 200 studies are posted on a weekday. The ceiling guards a runaway
# loop; reaching it is logged, never silently truncated.
MAX_RESULTS = 2000
# The registry posts nothing at weekends -- measured as 0 on Saturday and
# Sunday against 214-241 on weekdays. Failing twice a week would train everyone
# to ignore the alert; the manifest still records rows: 0.
ALLOW_EMPTY = True


def fetch(dt: str) -> Any:
    log = get_dagster_logger()
    pages: list[dict[str, Any]] = []
    token = None
    seen = 0
    total = None

    while seen < MAX_RESULTS:
        params = {
            "filter.advanced": f"AREA[StudyFirstPostDate]RANGE[{dt},{dt}]",
            "pageSize": PAGE_SIZE,
            "countTotal": "true",
        }
        if token:
            params["pageToken"] = token

        page = get_json(ENDPOINT, params=params, timeout=60)
        pages.append(page)
        if total is None:
            total = page.get("totalCount")
        seen += len(page.get("studies", []))

        token = page.get("nextPageToken")
        if not token:
            break

    if total and seen < total:
        log.warning(f"{dt}: registry reported {total} studies; kept {seen}")

    return {"total": total, "fetched": seen, "pages": pages}


def _dates(struct: dict[str, Any] | None) -> str | None:
    return (struct or {}).get("date")


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    records = []
    for page in payload["pages"]:
        for study in page.get("studies", []):
            p = study.get("protocolSection", {})
            ident = p.get("identificationModule", {})
            status = p.get("statusModule", {})
            design = p.get("designModule", {})
            sponsor = p.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
            eligibility = p.get("eligibilityModule", {})
            enrollment = design.get("enrollmentInfo", {})
            nct = ident.get("nctId")

            records.append(
                {
                    "dt": dt,
                    "nct_id": nct,
                    "title": ident.get("briefTitle"),
                    "status": status.get("overallStatus"),
                    "study_type": design.get("studyType"),
                    "phases": design.get("phases") or [],
                    "enrollment": enrollment.get("count"),
                    "enrollment_type": enrollment.get("type"),
                    "lead_sponsor": sponsor.get("name"),
                    "sponsor_class": sponsor.get("class"),
                    "conditions": p.get("conditionsModule", {}).get("conditions") or [],
                    "interventions": [
                        i.get("name")
                        for i in p.get("armsInterventionsModule", {}).get("interventions", [])
                        if i.get("name")
                    ],
                    "primary_outcomes": [
                        o.get("measure")
                        for o in p.get("outcomesModule", {}).get("primaryOutcomes", [])
                        if o.get("measure")
                    ],
                    "brief_summary": p.get("descriptionModule", {}).get("briefSummary"),
                    "sex": eligibility.get("sex"),
                    "minimum_age": eligibility.get("minimumAge"),
                    "maximum_age": eligibility.get("maximumAge"),
                    "healthy_volunteers": eligibility.get("healthyVolunteers"),
                    "countries": sorted(
                        {
                            loc.get("country")
                            for loc in p.get("contactsLocationsModule", {}).get("locations", [])
                            if loc.get("country")
                        }
                    ),
                    "start_date": _dates(status.get("startDateStruct")),
                    "completion_date": _dates(status.get("completionDateStruct")),
                    "first_posted": _dates(status.get("studyFirstPostDateStruct")),
                    "last_update_posted": _dates(status.get("lastUpdatePostDateStruct")),
                    "url": f"https://clinicaltrials.gov/study/{nct}",
                }
            )
    return records


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Clinical trials first registered on the partition date.",
)
def clinical_trials(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=ClinicalTrial,
        allow_empty=ALLOW_EMPTY,
    )
