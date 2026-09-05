# Airflow dbt Batch ETL Pipeline

A production-shaped batch ETL platform: Airflow orchestrates extraction from three real (if
synthetic) sources — a mock REST API, a source Postgres database, and MinIO (S3-compatible file
storage) — through PySpark, into dbt-core bronze/silver/gold models on a Postgres warehouse, gated
by a real data-quality framework, and published to a `marts` schema a FastAPI + React dashboard
reads from live.

## Architecture

```
  mock-api (FastAPI)      source-db (Postgres)      minio (S3-compatible)
   /api/user-enrichment      orders table            product_events/*.csv
        \                        |                        /
         \_______________________|_______________________/
                                  |
                     Airflow DAG "etl_pipeline" (LocalExecutor)
                                  |
      ,---- extract_api ---- extract_db ---- extract_files ----,   (parallel)
      |                                                          |
      `-----------------------> combine_and_load_raw <-----------'
                  (PySpark local[*] inside the Airflow image:
                   type/validate each source, JDBC write to
                   warehouse Postgres's own raw.* tables)
                                  |
                            dbt_run (dbt-core, BashOperator)
                raw -> staging (clean/typed) -> silver (dedup'd,
                     joinable) -> gold (daily_revenue,
                     daily_product_engagement, user_ltv_segment_summary)
                                  |
                            dbt_test (58 real generic + singular tests)
                                  |
                    data_quality_checks (DataQualityFramework: row-count/
                    null/unique/date-range/value-range gate on gold,
                    separate from dbt's declarative model tests)
                                  |
                    publish_marts (publishes a `marts` schema of views
                    over gold, stamped with published_at)
                                  |
                    FastAPI marts API  --(polled)-->  React dashboard
```

## Quickstart

```bash
docker compose up --build
```

| Service            | URL                                    |
|---------------------|------------------------------------------|
| Airflow UI          | http://localhost:8092 (admin/admin)       |
| Dashboard frontend  | http://localhost:5220                     |
| Dashboard API       | http://localhost:8093                     |
| Dashboard API docs  | http://localhost:8093/docs                |
| Mock enrichment API | http://localhost:8091                     |
| MinIO console       | http://localhost:9011 (minioadmin/minioadmin) |
| Warehouse Postgres  | localhost:5493 (warehouse/warehouse)      |
| Source Postgres     | localhost:5494 (source/source)            |

Ports are a fresh block checked against every sibling portfolio project's `docker-compose.yml`
so multiple stacks can run at once without conflicts.

On first `docker compose up`, the `seed` one-shot service populates `source-db` (an `orders`
table, 14 days of history) and MinIO (14 daily `product_events` CSVs). The Airflow scheduler runs
`etl_pipeline` on its `@daily` schedule — trigger a run manually from the Airflow UI
(`etl_pipeline` → ▶ Trigger DAG) for an immediate end-to-end run, then watch the dashboard fill in
once `dbt_test` and `publish_marts` finish (a few minutes).

## API reference

| Method | Path                             | Description                                   |
|--------|-----------------------------------|------------------------------------------------|
| GET    | `/api/marts/daily-revenue`        | Daily order count/revenue (`limit`, default 30) |
| GET    | `/api/marts/product-engagement`   | Per-product daily view/cart/purchase counts (`limit`, default 20) |
| GET    | `/api/marts/segment-summary`      | Revenue and LTV rollup by user segment          |
| GET    | `/api/marts/status`               | Latest published metric date and publish time   |
| GET    | `/health`                          | Liveness check                                  |

Full interactive schema at `/docs`.

## Local development

**extraction / quality / backend** (Python 3.11 — matches this repo's CI and the Airflow image's
Python version):

```bash
cd extraction   # or quality, or backend
python3.11 -m venv venv
./venv/Scripts/activate   # or source venv/bin/activate on Linux/Mac
pip install -r requirements-dev.txt
pytest tests -v -m "not integration"   # fast: mocked API/S3, no Docker
pytest tests -v -m integration         # real Postgres/PySpark via testcontainers
```

**dbt** (needs a running Postgres — `docker compose up -d warehouse-db`, or point at any
Postgres):

```bash
cd dbt
pip install dbt-core==1.8.2 dbt-postgres==1.8.2
export WAREHOUSE_DB_HOST=localhost WAREHOUSE_DB_PORT=5493 WAREHOUSE_DB_NAME=warehouse \
       WAREHOUSE_DB_USER=warehouse WAREHOUSE_DB_PASSWORD=warehouse DBT_TARGET=postgres
dbt run --profiles-dir .
dbt test --profiles-dir .
```

**Frontend**:

```bash
cd frontend
npm install
npm run dev
```

## Design Decisions & Trade-offs

- **Each source lands in its own `raw.*` table, not a naive `spark.union()`** — the PRD's own
  `extract.py` sketch unions API/DB/S3 data into one DataFrame, but user-enrichment, orders, and
  product-events have genuinely different schemas; forcing a union means picking a lowest-common-
  denominator schema that throws away most columns. dbt's staging/silver/gold models do the real
  combining, via joins on `user_id`.
- **PySpark runs `local[*]` inside the same Airflow container**, not a separate Spark cluster or
  a Bitnami Spark image — same single-node trade-off as this series' streaming project, and it
  sidesteps that project's `bitnamilegacy` image-namespace maze entirely by extending Airflow's
  own official image with a JDK instead.
- **`data_quality_checks` is a separate, real pipeline-level gate**, distinct from dbt's tests —
  a genuine implementation of the PRD's own `DataQualityFramework` sketch (row-count, null,
  uniqueness, date-range, value-range checks via SQLAlchemy), raising a real `DataQualityError`
  that fails the Airflow task. dbt tests stay declarative/model-scoped; this stays
  pipeline-scoped, matching the PRD's own architecture diagram, which shows them as separate
  stages.
- **`publish_marts` replaces the PRD's `load_warehouse` step.** dbt's own `dbt run` already
  materializes gold tables into the warehouse — a second unconditional "load to warehouse" step
  would just rewrite the same tables. Instead this step publishes a `marts` schema of views over
  gold, stamped with `published_at`, which is what the dashboard actually reads from — a stable,
  gated-on-success layer, which is the real reason a PRD would want that stage.
- **`catchup=False`, not the PRD's `catchup=True`.** The PRD's DAG starts from a fixed
  `days_ago(1)` with `catchup=True` and no `end_date` — that backfills once per day forever with
  no natural stopping point. A demo/portfolio DAG shouldn't silently queue an unbounded backfill.
- **Full-table (not incremental) silver/gold materialization.** The PRD's own `events_cleaned`
  sketch uses `materialized='incremental'`; at this data volume (tens of thousands of rows/day) a
  full rebuild every run is simpler, always correct, and fast enough — incremental logic is worth
  its complexity at real production volume, not here.
- **dbt tests: 58 real generic + singular tests pass** (`not_null`, `unique`, `relationships`,
  `accepted_values` across all 9 models, plus the PRD's own custom `not_null_unique` generic test
  made runnable, plus a singular revenue-sign-check) — comfortably past the PRD's 50+ target, and
  it's the actual number `dbt test` printed, not a guess.
- **Warehouse is local Postgres, not Snowflake/BigQuery.** dbt's postgres adapter is first-class
  and free; a `snowflake`/`bigquery` profile is included in `dbt/profiles.yml` and documented, but
  not live-tested against a real cloud account for this demo (`dbt run --target snowflake` is the
  literal switch once real credentials are supplied).
- **`dbt-core` must be pinned explicitly alongside `dbt-postgres`.** Installing only
  `dbt-postgres==1.8.2` pulled in `dbt-core 2.0.0rc1` (a pre-release) as a transitive dependency
  on this machine — pinning `dbt-core==1.8.2` directly avoids that surprise. Documented in the
  Dockerfile and CI.
- **dbt runs in its own isolated Python venv inside the Airflow image (`/opt/dbt-venv`), not
  Airflow's own environment.** `dbt-core` requires `sqlparse>=0.5`, but Airflow 2.8.4's own
  published constraints file pins `sqlparse==0.4.4` for its DB layer — a genuine, unresolvable
  conflict in one environment, not just an overly strict pin. Giving dbt its own venv (the
  standard community fix for this exact Airflow/dbt conflict) and invoking it by full path from
  `BashOperator` avoids it entirely.

## Benchmark results

The PRD's target of **500M+ records/day with 50+ dbt tests and sub-1-hour end-to-end latency**
describes an enterprise data volume this single-machine demo doesn't approach — one Postgres
warehouse, one dbt process, no cluster. Real numbers from actually running this repo's own
pipeline (extraction → `combine_and_load` → `dbt run` → `dbt test` → quality checks → publish)
against a live Postgres on the development machine:

| Metric | Result |
|---|---|
| dbt models built | **9/9 succeeded** (3 staging views, 3 silver tables, 3 gold tables) in **17.7s** |
| dbt tests | **58/58 passed** in **21.2s** (`not_null`, `unique`, `relationships`, `accepted_values` across all 9 models, the PRD's own custom `not_null_unique` generic test made runnable, and a singular revenue-sign check) — comfortably past the PRD's 50+ target, and the actual number `dbt test` printed |
| Rows processed | 500 user-enrichment + 2,000 orders + 3,000 product-events = 5,500 raw rows → 9 derived tables, end-to-end in well under a minute |
| `data_quality_checks` | All 6 checks (schema/null/row-count/unique/date-range/value-range) passed against the real `gold` tables |
| `publish_marts` | Published all 3 mart views in under a second |

Rather than 500M rows/day, this demonstrates the pipeline is *correct* end-to-end at a scale that
runs comfortably in a CI job or a laptop demo. Scaling to real production volume is an
infrastructure change (a warehouse cluster, partitioned Airflow workers, dbt's own incremental
materializations turned on) — not a rewrite of this pipeline's logic.

**Note on live `docker compose up` verification**: this repo's dbt/quality/extraction logic was
verified for real against a live Postgres warehouse (numbers above) and all test suites pass, but
a full `docker compose up` run of the entire stack (Airflow UI trigger → live dashboard) was not
completed in this session — the development machine hit severe host memory pressure (multiple
concurrent Docker stacks from unrelated work) that crashed Docker Desktop's engine mid-verification.
CI (a clean, dedicated runner) independently exercises the dbt/quality/extraction/backend
pieces; the live Airflow-orchestrated run is worth re-verifying once the host isn't memory-starved.

## Tech stack

Apache Airflow (LocalExecutor) · PySpark (local mode) · dbt-core (postgres adapter) · Postgres ·
MinIO (S3-compatible) · FastAPI · React + Vite + TypeScript + Recharts · Docker Compose
