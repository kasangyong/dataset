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

DAILY = DailyPartitionsDefinition(start_date=START_DATE, timezone="Asia/Seoul")


def yesterday_kst(now: datetime | None = None) -> str:
    """Partition key the scheduled run should target."""
    current = now or datetime.now(KST)
    return (current.astimezone(KST).date() - timedelta(days=1)).isoformat()


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
