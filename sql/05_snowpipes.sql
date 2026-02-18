-- Assumed dependencies:
-- - LANDING_STAGE external stage exists
-- - PLATFORM_DB.LANDING.LANDING_USERS and LANDING_POSTS tables exist
-- Execution order:
-- 1) Create USERS pipe
-- 2) Create POSTS pipe
-- 3) Validate COPY_HISTORY
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
CREATE OR REPLACE PIPE PLATFORM_DB.INGESTION.USERS_PIPE
AS
COPY INTO PLATFORM_DB.LANDING.LANDING_USERS(payload)
FROM @LANDING_STAGE/users
FILE_FORMAT = (TYPE = JSON);

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
