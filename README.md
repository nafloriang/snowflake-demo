# Snowflake Ingestion Foundation

This repository tracks the Snowflake ingestion objects deployed for `PLATFORM_DB`.

## Deployed Configuration

### Schemas

- `INGESTION`
- `LANDING`
- `METADATA`
- `STAGING`
- `DV_RAW`

### GCS

- Bucket: `gcs://autonomous-vault-landing/`
- Stage path: `gcs://autonomous-vault-landing/landing/`

## SQL Execution Order

1. `sql/01_create_database_and_schemas.sql`
2. `sql/02_storage_integration.sql`
3. `sql/03_stage_and_format.sql`
4. `sql/04_landing_tables.sql`
5. `sql/05_snowpipes.sql`
6. `sql/06_validation.sql`
