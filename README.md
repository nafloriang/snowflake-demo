# Metadata-Ready Data Platform PoC

This repository contains a foundation-level ingestion pattern from API-originated JSON files into Snowflake using Google Cloud Storage and Snowpipe.

## Objective

Implement a clean, modern ingestion and transformation pattern using:

- Google Cloud Storage (Landing Layer)
- Snowflake (External Stage + Snowpipe)
- Entity-based ingestion
- Raw landing tables (`VARIANT` + metadata)
- A foundation ready for dbt + Automated Data Vault

The project starts from an empty baseline and recreates the architecture with versioned SQL.

## Final Architecture

`API → GCS → Snowpipe → LANDING (VARIANT) → STAGING → Data Vault (AutomatedDV)`

## Snowflake Architecture

### Database

- `PLATFORM_DB`

### Schemas

- `INGESTION`
- `LANDING`
- `METADATA`
- `STAGING`
- `DV_RAW`

## Quickstart

Run the SQL scripts in order:

1. `sql/01_create_database_and_schemas.sql`
2. `sql/02_storage_integration.sql`
3. `sql/03_stage_and_format.sql`
4. `sql/04_landing_tables.sql`
5. `sql/05_snowpipes.sql`
6. `sql/06_validation.sql`

## Snowflake Build Steps

### 1) Create database and schemas

See: `sql/01_create_database_and_schemas.sql`

### 2) Configure storage integration (GCS)

See: `sql/02_storage_integration.sql`

After running `DESC STORAGE INTEGRATION gcs_int;`, grant the
`STORAGE_GCP_SERVICE_ACCOUNT` principal **Storage Object Viewer** access on the GCS bucket.

### 3) Create file format and external stage

See: `sql/03_stage_and_format.sql`

The stage points to:

- `gcs://slalom-demo-gcp/landing/`

### 4) Create landing tables

See: `sql/04_landing_tables.sql`

Landing tables are append-only and include metadata columns for file-level traceability:

- `payload VARIANT`
- `ingested_at TIMESTAMP`
- `filename STRING`
- `file_row_number NUMBER`

### 5) Create Snowpipes

See: `sql/05_snowpipes.sql`

Pipes included:

- `PIPE_INGEST_USERS` loading from `@LANDING_STAGE/users`
- `PIPE_INGEST_POSTS` loading from `@LANDING_STAGE/posts`

Both pipes include an initial refresh (`ALTER PIPE ... REFRESH`) for first-time loading.

### 6) Validate ingestion

See: `sql/06_validation.sql`

Validation checks:

- `SHOW PIPES`
- `SELECT` from `PLATFORM_DB.LANDING.LANDING_USERS`
- `SELECT` from `PLATFORM_DB.LANDING.LANDING_POSTS`

## Repository Structure

```text
data-platform/
│
├── ingestion/
├── metadata/
├── dbt/
├── orchestration/
├── generators/
└── docs/

sql/
├── 01_create_database_and_schemas.sql
├── 02_storage_integration.sql
├── 03_stage_and_format.sql
├── 04_landing_tables.sql
├── 05_snowpipes.sql
└── 06_validation.sql
```

## Design Principles

- Single external stage
- One pipe per entity
- Append-only landing
- Metadata columns for traceability
- Clear schema separation
- No procedural over-engineering

## Current Status

This PoC currently includes:

- Cloud Storage integration pattern
- Snowflake external stage
- Snowpipe per entity
- Raw landing layer
- Multi-entity ingestion
- Architectural separation ready for transformation

## Next Steps

1. Add dbt project scaffold
2. Build staging models that parse `VARIANT`
3. Implement Automated Data Vault (Hubs / Links / Satellites)
4. Optionally add Streams + Tasks for CDC
5. Optionally add orchestration (Airflow/Astro)
