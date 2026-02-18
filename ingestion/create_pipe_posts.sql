-- Assumed dependencies:
-- - PLATFORM_DB database and INGESTION/LANDING schemas exist
-- - LANDING_STAGE external stage points to landing/ path
-- - PLATFORM_DB.LANDING.LANDING_POSTS table exists
-- Execution order:
-- 1) Create landing table
-- 2) Create pipe
-- 3) Validate pipe status and loaded files
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
CREATE OR REPLACE PIPE PIPE_INGEST_POSTS AS
COPY INTO PLATFORM_DB.LANDING.LANDING_POSTS
(
    payload,
    filename,
    file_row_number
)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @LANDING_STAGE/posts
)
FILE_FORMAT = (TYPE = JSON);
