"""HTTP access for source fetchers.

Transient failures are retried by Dagster's RetryPolicy at the asset level, so
this module raises on error rather than swallowing it -- a raise is what lets
the retry happen.
"""

from __future__ import annotations

from typing import Any

import requests

DEFAULT_TIMEOUT = 30
USER_AGENT = "sangyong-datasets/0.1 (+https://github.com/)"


class FetchError(RuntimeError):
    """Raised when a source endpoint cannot be read."""


def get_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Any:
    merged = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if headers:
        merged.update(headers)

    try:
        response = requests.get(url, params=params, headers=merged, timeout=timeout)
    except requests.RequestException as exc:
        raise FetchError(f"request to {url} failed: {exc}") from exc

    if response.status_code != 200:
        body = response.text[:300]
        raise FetchError(f"{url} returned {response.status_code}: {body}")

    try:
        return response.json()
    except ValueError as exc:
        raise FetchError(f"{url} returned non-JSON body: {response.text[:300]}") from exc
