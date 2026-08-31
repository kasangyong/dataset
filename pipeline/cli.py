"""Entry point used by the scheduler.

Scheduling lives outside Dagster (GitHub Actions has no resident daemon), so
this driver decides which partitions to run and keeps one failing source from
taking the others down with it.

    python -m pipeline.cli                          # each source's due partition
    python -m pipeline.cli --group hourly           # only the hourly sources
    python -m pipeline.cli --start 2026-08-01 --end 2026-08-10   # backfill
    python -m pipeline.cli --sources fx_rates,hn_stories
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

from dagster import materialize

from pipeline.common import manifest, storage
from pipeline.common.partitions import START_DATE, partition_for
from pipeline.registry import BY_NAME, DAILY_SOURCES, HOURLY_SOURCES, SOURCES


# How far back a scheduled run reaches to repair partitions it already knows
# are missing. A transient upstream failure would otherwise leave a permanent
# hole: each run only fills its own day and never revisits.
HEAL_DAYS = 7


def find_gaps(names: list[str], days: int = HEAL_DAYS) -> list[tuple[str, str]]:
    """Recent partitions that failed or never landed.

    A partition counts as present only when the manifest says it succeeded and
    the file is there -- an empty file from a source that legitimately had no
    records is complete, a missing one is not.
    """
    gaps: list[tuple[str, str]] = []
    for name in names:
        source = BY_NAME[name]
        if not source.backfillable:
            # A snapshot source cannot be repaired after the fact; retrying it
            # would file today's numbers under a past date.
            continue

        due = date.fromisoformat(partition_for(source.lag_days))
        for back in range(1, days + 1):  # the due day itself is the normal plan
            dt = (due - timedelta(days=back)).isoformat()
            if dt < START_DATE:
                continue
            healthy = (
                manifest.latest_status(dt).get(name) == "ok"
                and storage.curated_path(name, dt).exists()
            )
            if not healthy:
                gaps.append((dt, name))
    return gaps


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


def build_plan(names: list[str], start: str | None, end: str | None) -> list[tuple[str, str]]:
    """Work out which (partition, source) pairs to run.

    Without an explicit range each source targets its own due partition, since
    a source's lag depends on whether it reports a completed day or a moment.
    """
    plan: list[tuple[str, str]] = []
    for name in names:
        source = BY_NAME[name]
        if not start:
            plan.append((partition_for(source.lag_days), name))
            continue

        for dt in date_range(start, end or start):
            if not source.backfillable and dt != partition_for(source.lag_days):
                # Writing a live snapshot under a past date would invent data.
                annotate("warning", f"{name} cannot be backfilled; skipping {dt}")
                continue
            plan.append((dt, name))
    return plan


def run(plan: list[tuple[str, str]], healing: list[tuple[str, str]] | None = None) -> int:
    """Materialize every planned pair. Returns a process exit code.

    Pairs in ``healing`` are repair attempts on partitions already known to be
    broken. They are tried on a best-effort basis and never decide the exit
    code: a source that is down upstream would otherwise paint every run red
    until it recovers.
    """
    summary = ["| date | source | status |", "| --- | --- | --- |"]
    outcomes: dict[str, dict[str, bool]] = {}

    for dt, name in healing or []:
        result = materialize([BY_NAME[name].asset], partition_key=dt, raise_on_error=False)
        verdict = "repaired" if result.success else "still failing"
        summary.append(f"| {dt} | {name} | {verdict} |")
        annotate("notice" if result.success else "warning", f"{name} {dt}: {verdict}")

    for dt, name in plan:
        # raise_on_error=False so one dead API cannot abort the rest of the
        # matrix; the failure is already recorded in the manifest.
        result = materialize([BY_NAME[name].asset], partition_key=dt, raise_on_error=False)
        outcomes.setdefault(dt, {})[name] = result.success
        summary.append(f"| {dt} | {name} | {'ok' if result.success else 'FAILED'} |")
        if not result.success:
            annotate("warning", f"{name} failed for {dt}")

    summarize(summary)

    dead_days = [dt for dt, results in outcomes.items() if not any(results.values())]
    if dead_days:
        # A green run with empty partitions is the failure mode this guards
        # against: nothing collected, nobody told.
        annotate("error", f"no data collected for: {', '.join(sorted(dead_days))}")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect dataset partitions.")
    parser.add_argument("--start", help=f"first partition date (>= {START_DATE})")
    parser.add_argument("--end", help="last partition date, inclusive")
    parser.add_argument("--sources", help="comma-separated source names; default all")
    parser.add_argument(
        "--no-heal",
        action="store_true",
        help="skip repairing recent failed or missing partitions",
    )
    parser.add_argument(
        "--group",
        choices=["all", "daily", "hourly"],
        default="all",
        help="which cadence of sources to run; ignored when --sources is given",
    )
    args = parser.parse_args(argv)

    if args.sources:
        names = [s.strip() for s in args.sources.split(",") if s.strip()]
        unknown = [s for s in names if s not in BY_NAME]
        if unknown:
            parser.error(f"unknown source(s): {', '.join(unknown)}. known: {', '.join(BY_NAME)}")
    elif args.group == "daily":
        names = list(DAILY_SOURCES)
    elif args.group == "hourly":
        names = list(HOURLY_SOURCES)
    else:
        names = [s.name for s in SOURCES]

    plan = build_plan(names, args.start, args.end)
    # Only a scheduled-shape run repairs history; an explicit range means the
    # caller is choosing the partitions themselves.
    healing = [] if (args.start or args.no_heal) else find_gaps(names)

    if not plan and not healing:
        annotate("warning", "nothing to collect")
        return 0

    print(f"collecting {len(plan)} partition-source pair(s)")
    if healing:
        print(f"repairing {len(healing)} known-bad partition(s) from the last {HEAL_DAYS} days")
    code = run(plan, healing)

    for dt in sorted({dt for dt, _ in plan}):
        print(f"{dt}: {manifest.latest_status(dt)}")
    return code


if __name__ == "__main__":
    sys.exit(main())
