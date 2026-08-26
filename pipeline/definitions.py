"""Dagster entry point.

Adding a source: create a module under ``sources/`` exposing an asset, then add
it to ``ASSETS`` below.
"""

from dagster import Definitions

from pipeline.sources.fx import fx_rates
from pipeline.sources.github_repos import github_repos
from pipeline.sources.hackernews import hn_stories

ASSETS = [fx_rates, github_repos, hn_stories]

defs = Definitions(assets=ASSETS)
