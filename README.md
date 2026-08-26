# sangyong_datasets

매일 정해진 시각에 외부 소스에서 데이터를 수집해 버전 관리되는 데이터셋으로 쌓는 파이프라인.

설계 문서: [docs/superpowers/specs/2026-08-26-daily-dataset-pipeline-design.md](docs/superpowers/specs/2026-08-26-daily-dataset-pipeline-design.md)

## 수집 소스

| 소스 | 내용 | 인증 | 일별 행 수 |
|---|---|---|---|
| `fx_rates` | ECB 기준 환율 (USD 기준, Frankfurter API) | 불필요 | ~29 |
| `github_repos` | 해당 날짜에 생성된 레포 (스타순 상위 100) | 선택 | 100 |
| `hn_stories` | Hacker News 해당 날짜 상위 글 | 불필요 | 100 |

Hacker News는 메타데이터와 링크만 저장하고 본문은 저장하지 않는다.

## 데이터 레이아웃

```
data/
  raw/<source>/dt=YYYY-MM-DD.json.gz    API 응답 원본
  curated/<source>/dt=YYYY-MM-DD.jsonl  정규화·검증된 레코드
  _manifest.jsonl                        실행 이력 (append-only)
```

파티션 키가 곧 파일 경로다. 같은 날짜를 몇 번 실행해도 해당 파일만 덮어쓰므로
중복이 쌓이지 않는다. 매니페스트만 누적되며, 재실행 이력 자체가 정보다.

모든 소스는 **D-1 (KST 전날)** 을 채운다. 날짜 라벨이 소스마다 다르면 조인이 불가능해지기 때문이다.

모든 레코드는 `collected_at` (UTC)을 가진다. `stars` 와 `points` 는 수집 시점에 한 번만
측정되므로, 관측 시각 없이는 값을 해석할 수 없다 — 6시간 뒤의 360점과 5일 뒤의 360점은
다른 사실이다. 백필한 파티션은 `dt` 가 가리키는 날이 아니라 백필을 실행한 시각에 측정된다.
`collected_at - created_at` 이 그 레코드의 관측 나이다.

시각으로 필터링하는 소스(`github_repos`, `hn_stories`)는 KST 자정 경계를 명시적으로 넘긴다.
기본값에 맡기면 UTC 하루를 가져와 `dt` 가 가리키는 날과 어긋난다.
`fx_rates` 는 ECB가 영업일당 하나의 환율을 발표하므로 시각 경계가 없고,
실제 발표일을 `rate_date` 에 남긴다 (주말·공휴일은 `is_stale: true`).

## 실행

```bash
uv sync --extra dev

uv run python -m pipeline.cli                                  # 전날, 전체 소스
uv run python -m pipeline.cli --start 2026-08-01 --end 2026-08-10   # 백필
uv run python -m pipeline.cli --sources fx_rates,hn_stories    # 일부만
```

종료 코드는 어느 날짜든 **모든** 소스가 실패하면 1, 그 외에는 0이다.
일부만 실패했을 때 0인 것은 의도된 것으로, 수집된 데이터는 커밋되어야 한다.

### Dagster UI

```bash
uv run dagster dev
```

자산 그래프, 파티션별 적재 상태, 백필을 UI에서 다룰 수 있다.

## 자동 실행

`.github/workflows/collect.yml` 이 매일 21:00 UTC (06:00 KST)에 돈다.
수집 결과는 `data/` 에 커밋된다.

수동 실행·백필은 Actions 탭의 `collect` → **Run workflow** 에서 날짜와 소스를 지정한다.

스케줄링을 Dagster 밖에 둔 이유는 설계 문서 2절에 있다. 요약하면 GitHub Actions에는
상주 데몬이 없어 Dagster 내장 스케줄러를 쓸 수 없다.

> GitHub은 부하 시 스케줄 실행을 지연시키고, 60일간 활동이 없는 레포의 스케줄은 비활성화한다.
> 매일 데이터가 커밋되므로 후자는 자연히 해소된다.

## 소스 추가하기

1. `pipeline/common/schema.py` 에 레코드 모델 추가
2. `pipeline/sources/<name>.py` 에 `fetch` / `normalize` / `@asset` 작성
   (기존 소스 파일을 그대로 베끼면 된다 — 공통 로직은 `common/collect.py` 에 있다)
3. `pipeline/definitions.py` 의 `ASSETS` 에 등록
4. `tests/fixtures/<name>.json` 에 실제 응답 픽스처 저장, `tests/test_normalize.py` 의 `CASES` 에 추가

## 테스트

```bash
uv run pytest
```

CI 게이트에는 네트워크 없이 도는 테스트만 포함한다. 외부 API 가용성이 CI 신뢰도를
흔들면 안 되기 때문이다. 실제 API를 때리는 테스트는 `@pytest.mark.network` 로 분리한다.

## 요구 사항

Python 3.14 (`.python-version` 으로 고정), uv.
버전 고정은 로컬과 CI를 같은 인터프리터로 맞추기 위한 것이다.
