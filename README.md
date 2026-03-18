# s3_snowflake_pipeline

A Dagster project that orchestrates a complete data pipeline: ingesting data from S3 into Snowflake using three different methods, transforming it with dbt Cloud, and syncing the results to external systems via Hightouch reverse ETL.

## Pipeline overview

```
S3 (parquet)  ──dlt───────►  raw_data/customers       ┐
S3 (parquet)  ──dlt───────►  raw_data/transactions    │
S3 (csv)      ──COPY INTO─►  raw_data/leads           ├─► dbt Cloud ─► mart_customers
S3 (csv)      ──sling─────►  target/raw_data/events   │
S3 (csv)      ──sling─────►  target/raw_data/products ┘
                                                              │
                            hightouch/salesforce_contacts_sync ◄─┤
                            hightouch/hubspot_companies_sync   ◄─┘
```

## Project structure

```
defs/
├── s3_ingestion/            # dlt: S3 parquet → Snowflake
│   ├── loads.py             # dlt sources and pipeline definition
│   └── defs.yaml            # DltLoadCollectionComponent config
├── sling_ingestion/         # Sling: S3 CSV → Snowflake
│   ├── replication.yaml     # Sling-native replication config
│   └── defs.yaml            # SlingReplicationCollectionComponent config
├── snowflake_s3_ingest/     # Snowflake COPY INTO: S3 CSV → Snowflake
│   ├── ingest.sql           # Jinja-templated SQL (CREATE TABLE + COPY INTO)
│   └── defs.yaml            # TemplatedSqlComponent config
├── dbt_cloud_transforms/    # dbt Cloud: transform raw → staging → marts
│   └── dbt_cloud_assets.py  # Pythonic @dbt_cloud_assets integration
├── hightouch_reverse_etl/   # Hightouch: Snowflake → Salesforce + HubSpot
│   └── defs.yaml            # Two HightouchSyncComponent instances (via ---)
├── snowflake_connection/    # Shared Snowflake SQLClient for TemplatedSqlComponent
│   └── defs.yaml            # SnowflakeConnectionComponent config
└── schedules/
    └── daily_pipeline.py    # 6 AM daily, selects group:ingestion+ (all downstream)
```

## Three ingestion approaches

This project demonstrates three ways to get data from S3 into Snowflake, each suited to different use cases:

### dlt (data load tool) — `s3_ingestion/`

Best for: schema evolution, incremental loading, complex sources, Python-level control over the pipeline.

dlt's `filesystem` source reads parquet files from S3 and loads them into Snowflake's `raw_data` dataset. A helper function in `loads.py` makes adding new sources a two-step process:

1. Add a source in `loads.py`: `orders_source = s3_source("orders/")`
2. Add an entry in `defs.yaml` referencing it

### Sling — `sling_ingestion/`

Best for: simple file-to-table replication (CSV, JSON) with no transformation logic. Configuration-only — no Python code required.

Sling uses its native `replication.yaml` format where you define S3 glob patterns as streams and Snowflake tables as targets. Connection strings for both S3 and Snowflake are configured in `defs.yaml`.

### Snowflake COPY INTO — `snowflake_s3_ingest/`

Best for: leveraging Snowflake-native S3 integration (external stages), maximum load performance, or when your team already manages Snowflake stages.

Uses `TemplatedSqlComponent` with a Jinja-templated SQL file that runs `CREATE TABLE IF NOT EXISTS`, `TRUNCATE`, and `COPY INTO` from a Snowflake external stage. All parameters (table name, stage, S3 prefix, file format) are configurable via `sql_template_vars` in `defs.yaml`.

This component references the `SnowflakeConnectionComponent` via `connection: "{{ context.load_component('snowflake_connection') }}"` — the `SnowflakeConnectionComponent` exists specifically to provide a `SQLClient` implementation that the `TemplatedSqlComponent` requires to execute queries.

## Transformation — dbt Cloud

The `dbt_cloud_transforms/` directory uses the pythonic `@dbt_cloud_assets` integration to connect to a dbt Cloud workspace. When materialized, it triggers a `dbt build` in dbt Cloud and maps each dbt model to a Dagster asset with proper lineage back to the raw ingestion tables.

dbt model asset keys (e.g. `stg_customers`, `mart_customers`) are derived from the dbt Cloud manifest. Your dbt `source()` definitions must reference the upstream asset keys produced by the ingestion components for the lineage to connect.

## Reverse ETL — Hightouch

The `hightouch_reverse_etl/defs.yaml` contains two `HightouchSyncComponent` instances in a single file using `---` YAML document separators:

- **Sync 84201** → `hightouch/salesforce_contacts_sync` — pushes `mart_customers` to Salesforce Contacts
- **Sync 84202** → `hightouch/hubspot_companies_sync` — pushes `mart_customers` to HubSpot Companies

Each `HightouchSyncComponent` represents **one specific Hightouch sync** (not all syncs at a destination). When materialized, it triggers the sync via the Hightouch API, polls for completion, and reports metadata (rows processed, failures, completion ratio). To add more syncs, add another `---` document to the same file.

## Schedule

A single daily schedule (`daily_pipeline_schedule`) runs at 6 AM UTC and selects `group:ingestion+` — all ingestion assets plus every downstream dependent. Dagster respects the dependency graph, so execution order is: ingestion (dlt + Sling + COPY INTO) → dbt Cloud transforms → Hightouch syncs.

## Getting started

### 1. Install dependencies

```bash
uv sync
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your real credentials
```

Required environment variables:

| Variable | Used by |
| --- | --- |
| `S3_BUCKET_URL` | dlt, TemplatedSqlComponent |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | S3 access |
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` | All Snowflake connections |
| `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE` | All Snowflake connections |
| `DBT_CLOUD_ACCOUNT_ID`, `DBT_CLOUD_API_TOKEN`, `DBT_CLOUD_PROJECT_ID`, `DBT_CLOUD_ENVIRONMENT_ID` | dbt Cloud |
| `HIGHTOUCH_API_KEY` | Hightouch syncs |

### 3. Run Dagster

```bash
uv run dg dev
```

Open http://localhost:3000 to see the asset graph and trigger materializations.

### 4. Run tests

```bash
uv run python -m pytest tests/ -v -s
```

The test suite validates that asset keys line up across all pipeline stages without requiring external service credentials.
