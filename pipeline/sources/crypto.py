"""Top cryptocurrencies by market cap (CoinGecko, no credentials).

This endpoint reports the market as it stands right now, not a daily close.
The registry marks the source unbackfillable for that reason: filing a live
snapshot under a past date would invent prices that never happened.
"""

from typing import Any

from dagster import AssetExecutionContext, Backoff, MaterializeResult, RetryPolicy, asset

from pipeline.common.collect import collect
from pipeline.common.http import get_json
from pipeline.common.partitions import DAILY_OPEN
from pipeline.common.schema import CryptoMarket

SOURCE = "crypto_markets"
ENDPOINT = "https://api.coingecko.com/api/v3/coins/markets"
PER_PAGE = 100


def fetch(dt: str) -> Any:
    return get_json(
        ENDPOINT,
        params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": PER_PAGE,
            "page": 1,
        },
    )


def normalize(payload: Any, dt: str) -> list[dict[str, Any]]:
    return [
        {
            "dt": dt,
            "coin_id": coin.get("id"),
            "symbol": coin.get("symbol"),
            "name": coin.get("name"),
            "price_usd": coin.get("current_price"),
            "market_cap": coin.get("market_cap"),
            "market_cap_rank": coin.get("market_cap_rank"),
            "volume_24h": coin.get("total_volume"),
            "high_24h": coin.get("high_24h"),
            "low_24h": coin.get("low_24h"),
            "change_pct_24h": coin.get("price_change_percentage_24h"),
            # CoinGecko's own timestamp for the quote, distinct from when we read it.
            "last_updated": coin.get("last_updated"),
        }
        for coin in payload
    ]


@asset(
    name=SOURCE,
    partitions_def=DAILY_OPEN,
    group_name="sources",
    retry_policy=RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL),
    description="Top 100 coins by market cap, as a snapshot at collection time.",
)
def crypto_markets(context: AssetExecutionContext) -> MaterializeResult:
    return collect(
        context,
        source=SOURCE,
        fetch=fetch,
        normalize=normalize,
        model=CryptoMarket,
    )
