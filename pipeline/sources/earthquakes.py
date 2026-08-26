"""Earthquakes recorded on the partition date (USGS FDSN, no credentials).

The only source here with coordinates: every record is an event at a place,
not a measurement of a series or a document.
"""

from typing import Any

from dagster import AssetExecutionContext, Backoff, MaterializeResult, RetryPolicy, asset

from pipeline.common.collect import collect
from pipeline.common.http import get_json
from pipeline.common.partitions import DAILY, day_bounds_iso
from pipeline.common.schema import Earthquake

SOURCE = "earthquakes"
ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"
# Below this the catalogue is dominated by dense-instrumentation regions, which
# says more about sensor placement than about seismicity.
MIN_MAGNITUDE = 2.5


def fetch(dt: str) -> Any:
    start, _ = day_bounds_iso(dt)
    end, _ = day_bounds_iso(_next_day(dt))
    return get_json(
        ENDPOINT,
        params={
            "format": "geojson",
            "starttime": start,
            "endtime": end,
            "minmagnitude": MIN_MAGNITUDE,
            "orderby": "time",
        },
        timeout=60,
    )


def _next_day(dt: str) -> str:
    from datetime import date, timedelta

    return (date.fromisoformat(dt) + timedelta(days=1)).isoformat()


def _epoch_ms_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(value / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    records = []
    for feature in payload.get("features", []):
        props = feature.get("properties", {})
        # GeoJSON orders coordinates longitude, latitude, depth.
        lon, lat, depth = (feature.get("geometry", {}).get("coordinates") or [None, None, None])[:3]
        records.append(
            {
                "dt": dt,
                "event_id": feature.get("id"),
                "occurred_at": _epoch_ms_to_iso(props.get("time")),
                "place": props.get("place"),
                "magnitude": props.get("mag"),
                "magnitude_type": props.get("magType"),
                "depth_km": depth,
                "latitude": lat,
                "longitude": lon,
                "tsunami": props.get("tsunami"),
                "felt_reports": props.get("felt"),
                "significance": props.get("sig"),
                "url": props.get("url"),
            }
        )
    return records


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description=f"Earthquakes of magnitude {MIN_MAGNITUDE}+ recorded on the partition date.",
)
def earthquakes(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=Earthquake,
    )
