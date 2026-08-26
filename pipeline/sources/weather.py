"""Daily weather for Korea's major cities (Open-Meteo archive, no credentials).

The archive series is reanalysis rather than a single station reading, so the
values are stable once published and a backfilled day matches a live one.
"""

from typing import Any

from dagster import AssetExecutionContext, Backoff, MaterializeResult, RetryPolicy, asset

from pipeline.common.collect import collect
from pipeline.common.http import get_json
from pipeline.common.partitions import DAILY
from pipeline.common.schema import CityWeather

SOURCE = "city_weather"
ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

# The eight administrative centres, which between them cover most of the
# population and every climate zone on the peninsula.
CITIES = {
    "서울": (37.5665, 126.9780),
    "부산": (35.1796, 129.0756),
    "인천": (37.4563, 126.7052),
    "대구": (35.8714, 128.6014),
    "대전": (36.3504, 127.3845),
    "광주": (35.1595, 126.8526),
    "울산": (35.5384, 129.3114),
    "제주": (33.4996, 126.5312),
}

DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "windspeed_10m_max",
]


def fetch(dt: str) -> Any:
    # One request per city: the endpoint takes a single coordinate pair.
    return {
        city: get_json(
            ENDPOINT,
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": dt,
                "end_date": dt,
                "daily": ",".join(DAILY_FIELDS),
                "timezone": "Asia/Seoul",
            },
        )
        for city, (lat, lon) in CITIES.items()
    }


def _first(daily: dict[str, Any], field: str) -> Any:
    values = daily.get(field) or [None]
    return values[0]


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    records = []
    for city, response in payload.items():
        daily = response.get("daily", {})
        records.append(
            {
                "dt": dt,
                "city": city,
                "latitude": response.get("latitude"),
                "longitude": response.get("longitude"),
                "temp_max_c": _first(daily, "temperature_2m_max"),
                "temp_min_c": _first(daily, "temperature_2m_min"),
                "temp_mean_c": _first(daily, "temperature_2m_mean"),
                "precipitation_mm": _first(daily, "precipitation_sum"),
                "windspeed_max_kmh": _first(daily, "windspeed_10m_max"),
            }
        )
    return records


@asset(
    name=SOURCE,
    partitions_def=DAILY,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Daily temperature, rainfall and wind for eight Korean cities.",
)
def city_weather(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=CityWeather,
    )
