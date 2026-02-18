-- Purpose:
-- Align PIPE_LANDING_USERS with POSTS ingestion pattern by loading payload plus file metadata.
-- Dependencies:
-- - PLATFORM_DB database exists.
-- - PLATFORM_DB.INGESTION.LANDING_USERS table exists with payload, filename, and file_row_number columns.
-- - LANDING_STAGE stage exists and contains users JSON files under users/.
-- Execution order:
-- 1) Run after landing table creation.
-- 2) Run before streams, procedures, and tasks.

CREATE OR REPLACE PIPE PLATFORM_DB.INGESTION.PIPE_LANDING_USERS AS
COPY INTO PLATFORM_DB.INGESTION.LANDING_USERS
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
    FROM @LANDING_STAGE/users
)
FILE_FORMAT = (TYPE = JSON);
