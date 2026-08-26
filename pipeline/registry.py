"""The catalog of sources.

Adding a source means adding one entry here. The fields beyond `asset` exist
because sources differ in ways that silently corrupt data if ignored: a live
snapshot backfilled into a past date writes today's numbers under yesterday's
label, and a feed that only holds the last two hours yields nothing at all if
collected once a day.
"""

from dataclasses import dataclass

from dagster import AssetsDefinition

from pipeline.sources import (
    arxiv,
    clinical_trials,
    crypto,
    earthquakes,
    fx,
    geeknews,
    github_repos,
    hackernews,
    weather,
    wikipedia,
    yna_news,
)


@dataclass(frozen=True)
class Source:
    name: str
    title: str  # Korean display name, used in the catalog
    asset: AssetsDefinition

    # Days between the partition date and the day the run happens. 1 means a
    # run collects yesterday, once that day is complete. 0 means the source has
    # no completed-day form -- it is a snapshot of the moment it is read.
    lag_days: int = 1

    # False when the source can only report its current state. Backfilling such
    # a source would file today's numbers under a past date.
    backfillable: bool = True

    # When set, a partition is merged rather than overwritten, keyed on this
    # field. Feeds that hold only a short window need many reads to cover a day.
    merge_key: str | None = None

    # Hourly rather than daily. Follows from a short feed window.
    hourly: bool = False


SOURCES = [
    Source("fx_rates", "달러 환율", fx.fx_rates),
    Source("github_repos", "깃허브 신규 저장소", github_repos.github_repos),
    Source("hn_stories", "해커뉴스 인기글", hackernews.hn_stories),
    Source("wikipedia_top", "위키백과 인기문서", wikipedia.wikipedia_top),
    Source("earthquakes", "지진 관측", earthquakes.earthquakes),
    Source("city_weather", "도시 날씨", weather.city_weather),
    Source("arxiv_papers", "arXiv 신규 논문", arxiv.arxiv_papers),
    # The registry posts by US Eastern date, which is still in progress when the
    # daily run fires. Two days back is the first complete one.
    Source("clinical_trials", "임상시험 신규 등록", clinical_trials.clinical_trials,
           lag_days=2),
    Source("crypto_markets", "암호화폐 시세", crypto.crypto_markets,
           lag_days=0, backfillable=False),
    Source("yna_news", "연합뉴스 헤드라인", yna_news.yna_news,
           lag_days=0, backfillable=False, merge_key="guid", hourly=True),
    # The feed holds fifty entries over roughly forty hours. That covers a day
    # today, but the window is a count not a duration -- a busy stretch would
    # shorten it. Hourly merging does not depend on posting volume.
    Source("geeknews", "GeekNews 개발뉴스", geeknews.geeknews,
           lag_days=0, backfillable=False, merge_key="topic_id", hourly=True),
]

BY_NAME = {s.name: s for s in SOURCES}
ASSETS = [s.asset for s in SOURCES]
DAILY_SOURCES = [s.name for s in SOURCES if not s.hourly]
HOURLY_SOURCES = [s.name for s in SOURCES if s.hourly]
