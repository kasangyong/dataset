"""ECB reference exchange rates via the Frankfurter API. No credentials needed."""

from typing import Any

from dagster import AssetExecutionContext, Backoff, MaterializeResult, RetryPolicy, asset

from pipeline.common.collect import collect
from pipeline.common.http import get_json
from pipeline.common.partitions import DAILY
from pipeline.common.schema import FxRate

SOURCE = "fx_rates"
BASE_CURRENCY = "USD"
ENDPOINT = "https://api.frankfurter.dev/v1/{date}"


def fetch(dt: str) -> Any:
    return get_json(ENDPOINT.format(date=dt), params={"base": BASE_CURRENCY})


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    # The ECB publishes on business days only. Asked for a weekend or holiday,
    # Frankfurter answers with the previous business day -- we keep that value
    # so the series has no gaps, and mark it so nobody mistakes it for fresh.
    rate_date = payload.get("date", "")
    base = payload.get("base", BASE_CURRENCY)
    is_stale = rate_date != dt

    return [
        {
            "dt": dt,
            "base": base,
            "quote": quote,
            "rate": rate,
            "rate_date": rate_date,
            "is_stale": is_stale,
        }
        for quote, rate in sorted(payload.get("rates", {}).items())
    ]


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Daily ECB reference rates against USD (Frankfurter API).",
)
def fx_rates(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=FxRate,
    )
