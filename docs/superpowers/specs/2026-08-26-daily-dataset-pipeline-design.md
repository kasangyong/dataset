# 일별 데이터셋 수집 파이프라인 — 설계

- 작성일: 2026-08-26
- 상태: 승인됨

## 1. 목적과 범위

매일 정해진 시각에 여러 외부 소스에서 데이터를 수집해 버전 관리되는 데이터셋으로 적재한다.

최종 활용처는 아직 정해지지 않았다. 따라서 이 설계의 성공 기준은 "특정 데이터셋을 잘 모으는 것"이 아니라
**새로운 소스를 낮은 비용으로 추가할 수 있고, 매일 사람 개입 없이 도는 뼈대를 만드는 것**이다.

### 범위에 포함
- 일별 파티션 단위 수집·정규화·적재
- 소스 3종 (환율/지수, GitHub 레포, Hacker News)
- GitHub Actions 기반 스케줄 실행 및 결과 커밋
- 실패 격리, 재시도, 실행 이력 기록
- 과거 날짜 백필

### 범위에서 제외
- 뉴스 본문 전문 수집 (저작권 검토 필요, 별도 과제)
- 데이터 분석·모델 학습·대시보드
- 데이터 웨어하우스 적재 (S3/BigQuery 등)

## 2. 기술 선택

### 오케스트레이터: Dagster

수집 대상이 "매일 하루치가 쌓이는 데이터셋"이므로 Dagster의 일별 파티션 자산
(`DailyPartitionsDefinition`)과 개념이 일치한다. 이 선택으로 다음이 따라온다.

- 백필(과거 날짜 채우기)이 프레임워크 기능으로 제공됨
- "어느 날짜가 비었는가"를 자산 상태로 추적 가능
- 자산별 재시도 정책을 선언적으로 지정

Prefect도 후보였으나 파티션·백필을 직접 구현해야 하므로 제외했다.

### 스케줄링: Dagster 외부 (GitHub Actions cron)

Dagster의 내장 스케줄러는 데몬 상주를 전제로 한다. GitHub Actions에는 상주 프로세스가 없고
러너가 매 실행마다 새 컨테이너이므로 내장 스케줄러를 쓸 수 없다.

따라서 스케줄링 책임을 프레임워크 밖으로 분리한다.

- GitHub Actions cron이 `dagster asset materialize` 를 호출
- Dagster는 자산 그래프·파티션·재시도·IO를 담당
- 로컬 개발 시에는 `dagster dev` 로 UI와 내장 스케줄러를 그대로 사용 가능

부작용: CI 실행 이력 DB가 매번 휘발한다. 이를 보완하기 위해 실행 이력을
`data/_manifest.jsonl` 에 파일로 남기고 레포에 커밋한다.

### Python 3.14 고정

`.python-version` 으로 3.14를 고정해 로컬과 CI를 동일 버전으로 맞춘다.
버전을 고정하는 이유는 재현성이며, 3.14 자체는 로컬 기본 인터프리터와 일치한다.

설계 검토 단계에서 "Dagster가 3.14를 지원하지 않는다"고 판단해 3.12를 고정했으나,
확인 결과 Dagster 1.13.19는 `>=3.10,<3.15` 를 선언하며 3.14에서 설치·실행 모두 정상이었다.
불필요한 버전 분기를 없애기 위해 3.14로 되돌렸다.

## 3. 아키텍처

```
sangyong_datasets/
  pipeline/
    definitions.py            # Dagster Definitions 진입점
    sources/
      fx.py                   # asset: fx_rates
      github_repos.py         # asset: github_repos
      hackernews.py           # asset: hn_stories
    common/
      schema.py               # pydantic 레코드 모델
      http.py                 # 재시도 포함 HTTP 클라이언트
      storage.py              # 경로 규약, raw/curated 기록
      manifest.py             # 실행 이력 append
  data/
    raw/<source>/dt=YYYY-MM-DD.json.gz
    curated/<source>/dt=YYYY-MM-DD.jsonl
    _manifest.jsonl
  tests/
  .github/workflows/collect.yml
```

### 자산 = 소스 1개

각 자산은 일별 파티션을 가지며 서로 의존하지 않는다. 한 소스의 실패가 다른 소스의
적재를 막지 않는다. 새 소스 추가 비용은 `sources/` 에 파일 1개 + `definitions.py` 에 등록 1줄이다.

### 데이터 흐름

```
외부 API → http.fetch (재시도)
         → storage.write_raw   (원본 그대로, gzip)
         → normalize (소스별)
         → schema 검증 (pydantic)
         → storage.write_curated (JSONL)
         → manifest.append (성공/실패, 행 수, 소요시간)
```

## 4. 저장 설계

| 계층 | 경로 | 포맷 | 근거 |
|---|---|---|---|
| raw | `data/raw/<source>/dt=YYYY-MM-DD.json.gz` | gzip JSON | 정규화 버그 시 재수집 없이 복구. 과거 데이터를 주지 않는 API(HN 순위 등)에는 유일한 복구 수단. 매일 새 파일이라 diff가 무의미하므로 압축이 순이득 (연 ~180MB → ~30MB) |
| curated | `data/curated/<source>/dt=YYYY-MM-DD.jsonl` | 평문 JSONL | 연 ~25MB로 작고, grep·diff·PR 리뷰가 가능해야 값어치가 있음. 스키마가 아직 안 굳었으므로 Parquet은 컬럼 확정 후로 미룸 |
| 이력 | `data/_manifest.jsonl` | 평문 JSONL | append-only. CI 이력 DB 휘발에 대한 보완 |

### 멱등성

파티션 키가 곧 파일 경로다. 같은 날짜를 몇 번 실행해도 해당 날짜 파일만 덮어쓰므로
중복 누적이 구조적으로 불가능하다.

매니페스트만 append-only이며, 같은 (source, dt) 조합이 여러 줄 존재할 수 있다.
이는 의도된 것으로, 재실행 이력 자체가 정보다. 조회 시 최신 줄을 취한다.

### 매니페스트 레코드

```json
{"source":"fx_rates","dt":"2026-08-25","status":"ok","rows":29,"invalid_rows":0,
 "bytes_raw":319,"bytes_curated":3302,"duration_s":0.5,"run_at":"2026-08-26T02:43:50Z","error":null}
```

## 5. 파티션 날짜 규약

모든 소스는 **D-1 (KST 기준 전날)** 을 채운다. 소스마다 기준일이 다르면 날짜로 조인할 수 없게 된다.

실행 시각은 매일 UTC 21:00 = KST 06:00. 전날이 완전히 종료된 뒤 수집한다.

주말·공휴일에 값이 없는 소스(ECB 환율)는 직전 영업일 값을 기록하고 `is_stale: true` 를 붙인다.
값을 비우면 시계열에 구멍이 생기고, 플래그 없이 채우면 거짓 데이터가 되기 때문이다.

### 타임존 처리

`dt` 는 KST 달력 날짜다. 시각으로 필터링하는 소스는 **KST 자정 경계**를 명시적으로 넘긴다.
기본값에 맡기면 소스마다 다른 하루를 가져와 `dt` 조인이 조용히 어긋난다.

| 소스 | 처리 |
|---|---|
| `github_repos` | `created:<dt>T00:00:00+09:00..<dt>T23:59:59+09:00`. 맨 날짜(`created:YYYY-MM-DD`)는 UTC 하루로 해석된다 |
| `hn_stories` | `created_at_i` 를 KST 자정 epoch 범위로 필터 |
| `fx_rates` | ECB가 영업일당 하나의 환율을 발표하므로 시각 경계가 없다. 실제 발표일을 `rate_date` 에 그대로 기록한다 |

`fx_rates` 의 `dt` 는 "그 날짜에 유효한 환율"을 뜻하며, 발표 시각이 아니다.
값이 실제로 언제 발표됐는지는 `rate_date` 와 `is_stale` 로 판단한다.

## 6. 소스 정의

소스는 `pipeline/registry.py` 한 곳에 선언한다. 이름과 자산 외에 세 가지 특성을 갖는데,
무시하면 데이터가 조용히 오염되기 때문에 명시적으로 둔다.

| 필드 | 뜻 |
|---|---|
| `lag_days` | 실행일과 파티션 날짜의 차이. 1이면 완결된 어제, 0이면 읽는 순간의 스냅샷 |
| `backfillable` | 현재 상태만 알려주는 소스는 False. 과거 날짜로 채우면 오늘 값이 과거 라벨을 단다 |
| `merge_key` | 설정하면 파티션을 덮어쓰지 않고 이 필드 기준으로 병합 |
| `hourly` | 피드 노출 창이 짧아 하루 한 번으로는 못 담는 소스 |

| 데이터셋 | id | 소스 | lag | 백필 | 주기 |
|---|---|---|---|---|---|
| 달러 환율 | `fx_rates` | Frankfurter (ECB) | 1 | 가능 | 일 |
| 깃허브 신규 저장소 | `github_repos` | GitHub Search API | 1 | 가능 | 일 |
| 해커뉴스 인기글 | `hn_stories` | HN Algolia | 1 | 가능 | 일 |
| arXiv 신규 논문 | `arxiv_papers` | arXiv Atom API | 1 | 가능 | 일 |
| 암호화폐 시세 | `crypto_markets` | CoinGecko | 0 | 불가 | 일 |
| 연합뉴스 헤드라인 | `yna_news` | 연합뉴스 RSS | 0 | 불가 | **시간** |

### 6.1 fx_rates

`api.frankfurter.dev/v1/{date}?base=USD`. 통화별 1행, 약 29행/일.
ECB는 영업일당 하나의 환율을 발표하므로 시각 경계가 없다. 실제 발표일을 `rate_date` 에
기록하고, `dt != rate_date` 이면 `is_stale: true`.

### 6.2 github_repos

`api.github.com/search/repositories`, `created:<KST 자정 범위>`, 스타순 100건.
토큰은 선택이지만 Actions 기본 토큰을 넘겨 rate limit 을 올린다.

### 6.3 hn_stories

`hn.algolia.com/api/v1/search`, `created_at_i` 를 KST 자정 epoch 범위로 필터. 상위 100건.
본문은 저장하지 않는다.

### 6.4 arxiv_papers

`export.arxiv.org/api/query`, cs.AI / cs.LG / cs.CL, 제출일 KST 범위. Atom XML.
100건씩 페이징하며 arXiv 요청 간격 권고(3초)를 지킨다. 상한 800건은 폭주 방지용이고
초과 시 로그로 알린다 — 조용히 자르지 않는다.

**초록을 저장한다.** arXiv 가 바로 그 용도로 배포하므로 뉴스 본문과 달리 라이선스 문제가 없다.
대신 용량이 크다: 실측 약 450KB/일로 전체의 85%를 차지하며 연 165MB 규모다.

### 6.5 crypto_markets

`api.coingecko.com/api/v3/coins/markets`, 시총 상위 100.
**일별 종가가 아니라 현재 시세다.** `last_updated` 가 수 분 전을 가리킨다.
따라서 `lag_days=0`, `backfillable=False`.

### 6.6 yna_news

연합뉴스 RSS. 제목·요약(리드)·링크만 저장하고 본문은 저장하지 않는다.

**피드가 약 1.5시간치만 담는다** (실측 120건이 88분 범위). 하루 한 번 읽으면 하루의 약 2%다.
그래서 매시간 읽어 `guid` 기준으로 그날 파티션에 병합한다. 이미 저장된 항목은 그대로 두므로
`collected_at` 은 "처음 본 시각"을 뜻한다.

병합 소스는 raw 도 읽기마다 따로 남긴다(`raw/<source>/dt=<날짜>/<타임스탬프>.json.gz`).
덮어쓰면 마지막 읽기만 복원 가능해져 raw 를 두는 이유가 사라진다.

### 6.7 진행 중인 날의 파티션

Dagster 의 `DailyPartitionsDefinition` 은 끝나지 않은 날을 유효한 파티션으로 보지 않는다.
`lag_days=0` 소스는 정의상 오늘 파티션이 필요하므로 `end_offset=1` 을 준 별도 정의
(`DAILY_OPEN`)를 쓴다. 완결된 날을 다루는 소스는 기존 `DAILY` 를 그대로 쓴다.

## 7. 에러 처리

3층으로 나눈다.

1. **HTTP 계층** — Dagster `RetryPolicy(max_retries=3, delay=5, backoff=EXPONENTIAL)`.
   일시적 네트워크 오류·5xx·rate limit 을 흡수한다.
2. **소스 간 격리** — 드라이버(`pipeline/cli.py`)가 소스마다 `materialize(..., raise_on_error=False)`
   로 실행한다. FX가 죽어도 HN·GitHub는 수집·커밋된다.

   설계 초안은 워크플로의 자산별 step + `continue-on-error` 였으나 Python 드라이버로 바꿨다.
   백필이 (날짜 × 소스) 이중 루프인데 YAML step으로는 표현할 수 없고, 격리 로직은
   테스트 대상이어야 하기 때문이다. `tests/test_cli.py` 가 이 동작을 검증한다.
3. **전멸 감지** — 드라이버가 어느 날짜에서 모든 소스가 실패하면 exit code 1을 반환해 잡을 실패시킨다.
   일부만 실패하면 exit 0으로 두어 수집된 데이터는 커밋한다.
   이 층이 없으면 API가 전부 죽은 날에도 워크플로가 초록불로 끝나고 빈 날짜가 조용히 쌓인다.

### 관측 시각 (`collected_at`)

모든 레코드는 소스를 실제로 읽은 UTC 시각을 `collected_at` 에 담는다.
`stars`·`points` 같은 카운트는 수집 시점에 한 번만 측정되므로, 관측 시각이 없으면
값 자체를 해석할 수 없다. 게시 6시간 뒤의 360점과 5일 뒤의 360점은 다른 사실이다.

- 스탬프는 `fetch` 가 응답을 받은 직후에 찍는다. 정규화 시점이 아니라 값이 참이었던 순간이다.
- 한 번의 수집은 하나의 관측이므로 그 배치의 모든 레코드가 같은 값을 갖는다.
- 정규화 함수가 아니라 수집기(`collect`)가 찍는다. 소스마다 따로 챙기면 빠뜨린다.
- `collected_at - created_at` 이 그 레코드의 관측 나이다.

정규 스케줄로 모은 파티션은 관측 나이가 6~30시간이고, 백필한 파티션은 백필 시점까지
값이 익은 상태다. 이 둘을 한 시계열에 섞으면 측정 방식의 변화가 추세로 오독된다.
`collected_at` 은 그 구분을 행 단위로 가능하게 한다.

### 스키마 검증 실패

pydantic 검증에 실패한 레코드는 curated 에서 제외하되 버리지 않는다.
`invalid_rows` 카운트를 매니페스트에 남기고, 원본은 raw 에 이미 보존되어 있다.
검증 실패가 전체의 50%를 넘으면 해당 소스를 실패로 처리한다 — API 스키마가 바뀐 신호이기 때문이다.

## 8. 백필

`workflow_dispatch` 입력으로 시작일·종료일·대상 소스를 받아 과거 파티션을 채운다.

- HN, GitHub: 과거 조회 가능
- FX: Frankfurter 가 제공하는 히스토리 범위 내에서 가능

로컬에서는 `dagster asset materialize --select <asset> --partition YYYY-MM-DD` 로 동일하게 수행한다.

## 9. 테스트 전략

CI 게이트에는 **네트워크 없이 도는 테스트만** 포함한다. 외부 API 가용성이 CI 신뢰도를 흔들면 안 된다.

| 대상 | 방식 |
|---|---|
| 정규화 로직 | 저장된 응답 픽스처 → normalize → 기대 레코드 비교 |
| 스키마 검증 | 필드 누락·타입 불일치 케이스가 걸러지는지 |
| 경로 규약 | 파티션 키 → 파일 경로 매핑 |
| 매니페스트 | 성공·실패 각각 올바른 줄이 append 되는지 |
| 멱등성 | 같은 파티션 2회 실행 시 행이 중복되지 않는지 |

실제 API 호출은 `@pytest.mark.network` 로 분리해 수동 실행한다.

## 10. 성공 기준

1. `pytest` 전체 통과 (네트워크 불필요)
2. 로컬에서 3개 자산 모두 실제 수집 성공, `data/` 에 raw·curated·manifest 생성 확인
3. 같은 파티션 재실행 시 파일이 덮어써지고 행 수가 동일 (멱등성)
4. 소스 1개를 강제로 실패시켜도 나머지 2개가 정상 적재됨
5. GitHub Actions 워크플로가 수동 실행(`workflow_dispatch`)으로 성공하고 결과가 커밋됨

## 11. 검증 결과 (2026-08-26)

| 기준 | 결과 |
|---|---|
| 1. `pytest` 전체 통과 (네트워크 불필요) | 47 passed |
| 2. 3개 자산 실제 수집 | fx_rates 29행 / github_repos 100행 / hn_stories 100행 |
| 3. 멱등성 (동일 파티션 재실행) | 행 수 동일, 신규 파일 없음, 매니페스트만 누적 |
| 4. 소스 1개 강제 실패 시 격리 | fx_rates 실패, 나머지 2개 정상 적재, exit 0 |
| 5. GitHub Actions 실행 | 원격 레포 생성 후 확인 필요 |

재시도 정책도 함께 확인됐다. 강제 실패 시 매니페스트에 4줄(최초 1회 + 재시도 3회)이 남았다.

일별 실측 용량은 raw 64KB + curated 80KB = 약 144KB, 연 환산 약 52MB다.

## 12. 미결 사항 (후속 과제)

- **arXiv 용량.** 초록 때문에 연 165MB 로 전체의 85%다. 커지면 이 소스만 curated 를
  gzip 하거나 카테고리를 줄인다. 지금은 그대로 둔다.
- **시간당 커밋 빈도.** `yna_news` 때문에 하루 최대 24 커밋이 생긴다. 필요하면 주기를
  2~3시간으로 늘린다. 피드 창이 1.5시간이므로 그 이상은 누락이 생긴다.
- 데이터 리텐션 정책. 현재는 무기한 보관. 레포가 커지면 raw 를 N일 후 삭제하거나 데이터 레포를 분리한다.
- 최종 활용처가 정해지면 curated 를 Parquet 으로 변환하는 단계를 추가할지 결정한다.
- 뉴스 본문 코퍼스는 라이선스 검토 후 별도 소스로 추가한다.
