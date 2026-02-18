-- Assumed dependencies:
-- - PLATFORM_DB database and LANDING schema exist
-- Execution order:
-- 1) Create LANDING_USERS
-- 2) Create LANDING_POSTS
-- 3) Load or ingest data
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test

CREATE OR REPLACE TABLE PLATFORM_DB.INGESTION.LANDING_USERS(
    payload VARIANT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    filename STRING,
    file_row_number NUMBER
);


CREATE OR REPLACE TABLE PLATFORM_DB.INGESTION.LANDING_POSTS (
    payload VARIANT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    filename STRING,
    file_row_number NUMBER
);
