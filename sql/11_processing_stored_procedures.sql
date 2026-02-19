-- Purpose:
-- Create deterministic stream-processing procedures to normalize and deduplicate users and posts.
-- Dependencies:
-- - PLATFORM_DB.PROCESSING schema exists.
-- - PLATFORM_DB.PROCESSING.USERS_TABULAR and PLATFORM_DB.PROCESSING.POSTS_TABULAR exist.
-- - PLATFORM_DB.INGESTION.LANDING_USERS_STREAM and PLATFORM_DB.INGESTION.LANDING_POSTS_STREAM exist.
-- Execution order:
-- 1) Run after schemas, tabular tables, and streams are created.
-- 2) Run before orchestration tasks are created.

CREATE OR REPLACE PROCEDURE PLATFORM_DB.PROCESSING.SP_PROCESS_USERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN

    TRUNCATE TABLE PLATFORM_DB.PROCESSING.USERS_TABULAR;

    INSERT INTO PLATFORM_DB.PROCESSING.USERS_TABULAR (
        user_id,
        name,
        username,
        email,
        phone,
        website,
        load_ts,
        source_file
    )
    SELECT
        user_id,
        name,
        username,
        email,
        phone,
        website,
        load_ts,
        source_file
    FROM (
        SELECT
            f.value:id::NUMBER        AS user_id,
            f.value:name::STRING      AS name,
            f.value:username::STRING  AS username,
            f.value:email::STRING     AS email,
            f.value:phone::STRING     AS phone,
            f.value:website::STRING   AS website,
            s.ingested_at             AS load_ts,
            s.filename                AS source_file,
            ROW_NUMBER() OVER (
                PARTITION BY f.value:id::NUMBER
                ORDER BY s.ingested_at DESC
            ) AS rn
        FROM PLATFORM_DB.INGESTION.LANDING_USERS_STREAM s,
             LATERAL FLATTEN(input => s.payload) f
        WHERE METADATA$ACTION = 'INSERT'
          AND f.value:id IS NOT NULL
    )
    WHERE rn = 1;

    RETURN 'SP_PROCESS_USERS completed successfully';

END;
$$;

CREATE OR REPLACE PROCEDURE PLATFORM_DB.PROCESSING.SP_PROCESS_POSTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN

    -- Para demo limpiamos destino
    TRUNCATE TABLE PLATFORM_DB.PROCESSING.POSTS_TABULAR;

    INSERT INTO PLATFORM_DB.PROCESSING.POSTS_TABULAR (
        post_id,
        user_id,
        title,
        body,
        load_ts,
        source_file
    )
    SELECT
        post_id,
        user_id,
        title,
        body,
        load_ts,
        source_file
    FROM (
        SELECT
            f.value:id::NUMBER         AS post_id,
            f.value:userId::NUMBER     AS user_id,
            f.value:title::STRING      AS title,
            f.value:body::STRING       AS body,
            s.ingested_at              AS load_ts,
            s.filename                 AS source_file,
            ROW_NUMBER() OVER (
                PARTITION BY f.value:id::NUMBER
                ORDER BY s.ingested_at DESC
            ) AS rn
        FROM PLATFORM_DB.INGESTION.LANDING_POSTS_STREAM s,
             LATERAL FLATTEN(input => s.payload) f
        WHERE METADATA$ACTION = 'INSERT'
          AND f.value:id IS NOT NULL
    )
    WHERE rn = 1;

    RETURN 'SP_PROCESS_POSTS completed successfully';

END;
$$;
