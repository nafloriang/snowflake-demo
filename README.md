# Snowflake Ingestion Foundation

This repository contains a minimal Proof of Concept for a modern Snowflake-based data platform in `PLATFORM_DB`.

## Schemas

- `INGESTION`
- `LANDING`
- `METADATA`
- `STAGING`
- `DV_RAW`

## Run ingestion scripts

Run in order:

1. `sql/01_create_database_and_schemas.sql`
2. `sql/02_storage_integration.sql`
3. `sql/03_stage_and_format.sql`
4. `sql/04_landing_tables.sql`
5. `sql/05_snowpipes.sql`
6. `ingestion/create_pipe_posts.sql`
7. `ingestion/create_streams.sql`
8. `ingestion/create_task.sql`
9. `metadata/create_entity_config.sql`

## Refresh pipes

Use Snowflake SQL to refresh any pipe after backfilling files:

```sql
ALTER PIPE PLATFORM_DB.INGESTION.PIPE_INGEST_USERS REFRESH;
ALTER PIPE PLATFORM_DB.INGESTION.PIPE_INGEST_POSTS REFRESH;
```

## Run dbt

From `dbt/`:

```bash
cp profiles.yml.example ~/.dbt/profiles.yml

dbt debug
dbt run
dbt test
```

## Query DV Raw models

```sql
SELECT * FROM PLATFORM_DB.DV_RAW.HUB_USER LIMIT 10;
SELECT * FROM PLATFORM_DB.DV_RAW.HUB_POST LIMIT 10;
SELECT * FROM PLATFORM_DB.DV_RAW.LINK_USER_POST LIMIT 10;
SELECT * FROM PLATFORM_DB.DV_RAW.SAT_USER LIMIT 10;
SELECT * FROM PLATFORM_DB.DV_RAW.SAT_POST LIMIT 10;
```
