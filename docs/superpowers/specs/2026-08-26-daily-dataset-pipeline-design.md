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

## 6. 소스 정의

### 6.1 fx_rates — 환율

- API: Frankfurter (ECB 기준 환율), 인증 불필요
- 엔드포인트: `https://api.frankfurter.dev/v1/{date}?base=USD`
- 산출: 통화별 1행, 약 30행/일
- 필드: `dt`, `base`, `quote`, `rate`, `rate_date`, `is_stale`
- `rate_date` 는 API가 실제로 반환한 날짜. `dt != rate_date` 이면 `is_stale=true`

### 6.2 github_repos — 당일 생성된 인기 레포

- API: GitHub Search API, 인증 선택 (토큰 있으면 rate limit 10배)
- 쿼리: `created:YYYY-MM-DD`, `sort=stars`, 최대 100건
- 산출: 레포당 1행
- 필드: `dt`, `full_name`, `owner`, `language`, `stars`, `forks`, `description`, `topics`, `created_at`, `html_url`
- 토큰은 `GITHUB_TOKEN` 시크릿 사용 (Actions 기본 제공 토큰)

### 6.3 hn_stories — Hacker News 전일 상위 글

- API: HN Algolia Search, 인증 불필요
- 엔드포인트: `https://hn.algolia.com/api/v1/search_by_date` + 날짜 범위 필터
- 산출: 상위 100건
- 필드: `dt`, `object_id`, `title`, `url`, `author`, `points`, `num_comments`, `created_at`
- **본문은 저장하지 않는다.** 메타데이터와 링크만 저장해 저작권 문제를 처음부터 회피한다.

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

- 데이터 리텐션 정책. 현재는 무기한 보관. 레포가 커지면 raw 를 N일 후 삭제하거나 데이터 레포를 분리한다.
- 최종 활용처가 정해지면 curated 를 Parquet 으로 변환하는 단계를 추가할지 결정한다.
- 뉴스 본문 코퍼스는 라이선스 검토 후 별도 소스로 추가한다.
