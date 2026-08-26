"""Dagster entry point.

The source catalog lives in ``pipeline/registry.py``; adding a source means
adding one entry there.
"""

from dagster import Definitions

from pipeline.registry import ASSETS

defs = Definitions(assets=ASSETS)
