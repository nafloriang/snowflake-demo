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

    MERGE INTO PLATFORM_DB.PROCESSING.USERS_TABULAR AS tgt
    USING (
        WITH src AS (
            SELECT
                payload:id::NUMBER AS user_id,
                payload:name::STRING AS name,
                payload:username::STRING AS username,
                payload:email::STRING AS email,
                payload:phone::STRING AS phone,
                payload:website::STRING AS website,
                ingested_at AS load_ts,
                filename AS source_file,
                ROW_NUMBER() OVER (
                    PARTITION BY payload:id::NUMBER
                    ORDER BY ingested_at DESC
                ) AS rn
            FROM PLATFORM_DB.INGESTION.LANDING_USERS_STREAM
            WHERE METADATA$ACTION = 'INSERT'
        )
        SELECT *
        FROM src
        WHERE rn = 1
    ) AS src
    ON tgt.user_id = src.user_id

    WHEN MATCHED THEN UPDATE SET
        name = src.name,
        username = src.username,
        email = src.email,
        phone = src.phone,
        website = src.website,
        load_ts = src.load_ts,
        source_file = src.source_file

    WHEN NOT MATCHED THEN INSERT (
        user_id,
        name,
        username,
        email,
        phone,
        website,
        load_ts,
        source_file
    )
    VALUES (
        src.user_id,
        src.name,
        src.username,
        src.email,
        src.phone,
        src.website,
        src.load_ts,
        src.source_file
    );

    RETURN 'SP_PROCESS_USERS completed';

END;
$$;

CREATE OR REPLACE PROCEDURE PLATFORM_DB.PROCESSING.SP_PROCESS_POSTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN

    MERGE INTO PLATFORM_DB.PROCESSING.POSTS_TABULAR AS tgt
    USING (
        WITH src AS (
            SELECT
                payload:id::NUMBER AS post_id,
                payload:userId::NUMBER AS user_id,
                payload:title::STRING AS title,
                payload:body::STRING AS body,
                ingested_at AS load_ts,
                filename AS source_file,
                ROW_NUMBER() OVER (
                    PARTITION BY payload:id::NUMBER
                    ORDER BY ingested_at DESC
                ) AS rn
            FROM PLATFORM_DB.INGESTION.LANDING_POSTS_STREAM
            WHERE METADATA$ACTION = 'INSERT'
        )
        SELECT *
        FROM src
        WHERE rn = 1
    ) AS src
    ON tgt.post_id = src.post_id

    WHEN MATCHED THEN UPDATE SET
        user_id = src.user_id,
        title = src.title,
        body = src.body,
        load_ts = src.load_ts,
        source_file = src.source_file

    WHEN NOT MATCHED THEN INSERT (
        post_id,
        user_id,
        title,
        body,
        load_ts,
        source_file
    )
    VALUES (
        src.post_id,
        src.user_id,
        src.title,
        src.body,
        src.load_ts,
        src.source_file
    );

    RETURN 'SP_PROCESS_POSTS completed';

END;
$$;
