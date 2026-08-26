"""Partition definition shared by every source.

One partition = one calendar day in KST. Sources all fill D-1 so that records
from different sources on the same ``dt`` describe the same day and can be
joined.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from dagster import DailyPartitionsDefinition

KST = ZoneInfo("Asia/Seoul")
START_DATE = "2026-08-01"

# Completed days only. A source that reports a finished day has nothing to
# say about today until today ends.
DAILY = DailyPartitionsDefinition(start_date=START_DATE, timezone="Asia/Seoul")

# Includes the day in progress, for sources that only ever report "now".
# Without end_offset Dagster rejects today's key as not yet a partition.
DAILY_OPEN = DailyPartitionsDefinition(
    start_date=START_DATE, timezone="Asia/Seoul", end_offset=1
)


def partition_for(lag_days: int, now: datetime | None = None) -> str:
    """Partition key a scheduled run should target for a source.

    Sources differ: a completed-day source lags by one day, while a live
    snapshot has no completed-day form and belongs to the day it is read.
    """
    current = now or datetime.now(KST)
    return (current.astimezone(KST).date() - timedelta(days=lag_days)).isoformat()


def day_bounds_iso(dt: str) -> tuple[str, str]:
    """Inclusive ISO-8601 bounds of a KST calendar day, with offset.

    For sources that take a date string: passing a bare date would be read in
    the source's own timezone, which is not the one the partition label means.
    """
    return f"{dt}T00:00:00+09:00", f"{dt}T23:59:59+09:00"


def day_bounds_epoch(dt: str) -> tuple[int, int]:
    """[start, end) unix timestamps for a KST calendar day.

    Sources that filter by timestamp need the window that matches the partition
    label, not a UTC day that would straddle two labels.
    """
    day = date.fromisoformat(dt)
    start = datetime(day.year, day.month, day.day, tzinfo=KST)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())
