"""Entry point used by the scheduler.

Scheduling lives outside Dagster (GitHub Actions has no resident daemon), so
this driver decides which partitions to run and keeps one failing source from
taking the others down with it.

    python -m pipeline.cli                          # yesterday, all sources
    python -m pipeline.cli --start 2026-08-01 --end 2026-08-10
    python -m pipeline.cli --sources fx_rates,hn_stories
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

from dagster import materialize

from pipeline.common import manifest
from pipeline.common.partitions import START_DATE, yesterday_kst
from pipeline.definitions import ASSETS

BY_NAME = {asset.key.to_user_string(): asset for asset in ASSETS}


def date_range(start: str, end: str) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if last < first:
        raise ValueError(f"end {end} precedes start {start}")
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def annotate(level: str, message: str) -> None:
    """Surface a message in the Actions log; plain print elsewhere."""
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::{level}::{message}")
    else:
        print(f"[{level}] {message}")


def summarize(lines: list[str]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def run(dts: list[str], sources: list[str]) -> int:
    """Materialize every (source, date) pair. Returns a process exit code."""
    summary = ["| date | source | status |", "| --- | --- | --- |"]
    dead_days: list[str] = []

    for dt in dts:
        outcomes: dict[str, bool] = {}
        for name in sources:
            # raise_on_error=False so one dead API cannot abort the rest of the
            # matrix; the failure is already recorded in the manifest.
            result = materialize([BY_NAME[name]], partition_key=dt, raise_on_error=False)
            outcomes[name] = result.success
            summary.append(f"| {dt} | {name} | {'ok' if result.success else 'FAILED'} |")
            if not result.success:
                annotate("warning", f"{name} failed for {dt}")

        if not any(outcomes.values()):
            dead_days.append(dt)
            annotate("error", f"every source failed for {dt}")

    summarize(summary)

    if dead_days:
        # A green run with empty partitions is the failure mode this guards
        # against: nothing collected, nobody told.
        annotate("error", f"no data collected for: {', '.join(dead_days)}")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect daily dataset partitions.")
    parser.add_argument("--start", help=f"first partition date (>= {START_DATE})")
    parser.add_argument("--end", help="last partition date, inclusive")
    parser.add_argument("--sources", help="comma-separated source names; default all")
    args = parser.parse_args(argv)

    start = args.start or yesterday_kst()
    end = args.end or start
    dts = date_range(start, end)

    if args.sources:
        sources = [s.strip() for s in args.sources.split(",") if s.strip()]
        unknown = [s for s in sources if s not in BY_NAME]
        if unknown:
            parser.error(f"unknown source(s): {', '.join(unknown)}. known: {', '.join(BY_NAME)}")
    else:
        sources = list(BY_NAME)

    print(f"collecting {len(sources)} source(s) over {len(dts)} day(s): {dts[0]}..{dts[-1]}")
    code = run(dts, sources)

    for dt in dts:
        print(f"{dt}: {manifest.latest_status(dt)}")
    return code


if __name__ == "__main__":
    sys.exit(main())
