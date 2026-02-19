-- Purpose:
-- Create deterministic stream-processing and backfill procedures to normalize and deduplicate users and posts.
-- Dependencies:
-- - PLATFORM_DB.PROCESSING schema exists.
-- - PLATFORM_DB.PROCESSING.USERS_TABULAR and PLATFORM_DB.PROCESSING.POSTS_TABULAR exist.
-- - PLATFORM_DB.INGESTION.LANDING_USERS, PLATFORM_DB.INGESTION.LANDING_POSTS tables exist.
-- - PLATFORM_DB.INGESTION.LANDING_USERS_STREAM and PLATFORM_DB.INGESTION.LANDING_POSTS_STREAM streams exist.

CREATE OR REPLACE PROCEDURE PLATFORM_DB.PROCESSING.SP_PROCESS_USERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    MERGE INTO PLATFORM_DB.PROCESSING.USERS_TABULAR AS tgt
    USING (
        WITH parsed_stream AS (
            SELECT
                payload:id::NUMBER AS user_id,
                payload:name::STRING AS name,
                payload:username::STRING AS username,
                payload:email::STRING AS email,
                payload:phone::STRING AS phone,
                payload:website::STRING AS website,
                ingested_at AS load_ts,
                filename AS source_file,
                file_row_number,
                ROW_NUMBER() OVER (
                    PARTITION BY payload:id::NUMBER
                    ORDER BY ingested_at DESC, file_row_number DESC, filename DESC
                ) AS rn
            FROM PLATFORM_DB.INGESTION.LANDING_USERS_STREAM
            WHERE METADATA$ACTION = 'INSERT'
              AND COALESCE(METADATA$ISUPDATE, FALSE) = FALSE
              AND payload:id::NUMBER IS NOT NULL
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
        FROM parsed_stream
        WHERE rn = 1
    ) AS src
    ON tgt.user_id = src.user_id
    WHEN MATCHED THEN UPDATE SET
        tgt.name = src.name,
        tgt.username = src.username,
        tgt.email = src.email,
        tgt.phone = src.phone,
        tgt.website = src.website,
        tgt.load_ts = src.load_ts,
        tgt.source_file = src.source_file
    WHEN NOT MATCHED AND src.user_id IS NOT NULL THEN INSERT (
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
        WITH parsed_stream AS (
            SELECT
                payload:id::NUMBER AS post_id,
                payload:userId::NUMBER AS user_id,
                payload:title::STRING AS title,
                payload:body::STRING AS body,
                ingested_at AS load_ts,
                filename AS source_file,
                file_row_number,
                ROW_NUMBER() OVER (
                    PARTITION BY payload:id::NUMBER
                    ORDER BY ingested_at DESC, file_row_number DESC, filename DESC
                ) AS rn
            FROM PLATFORM_DB.INGESTION.LANDING_POSTS_STREAM
            WHERE METADATA$ACTION = 'INSERT'
              AND COALESCE(METADATA$ISUPDATE, FALSE) = FALSE
              AND payload:id::NUMBER IS NOT NULL
        )
        SELECT
            post_id,
            user_id,
            title,
            body,
            load_ts,
            source_file
        FROM parsed_stream
        WHERE rn = 1
    ) AS src
    ON tgt.post_id = src.post_id
    WHEN MATCHED THEN UPDATE SET
        tgt.user_id = src.user_id,
        tgt.title = src.title,
        tgt.body = src.body,
        tgt.load_ts = src.load_ts,
        tgt.source_file = src.source_file
    WHEN NOT MATCHED AND src.post_id IS NOT NULL THEN INSERT (
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

CREATE OR REPLACE PROCEDURE PLATFORM_DB.PROCESSING.SP_BACKFILL_USERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    MERGE INTO PLATFORM_DB.PROCESSING.USERS_TABULAR AS tgt
    USING (
        WITH parsed_landing AS (
            SELECT
                payload:id::NUMBER AS user_id,
                payload:name::STRING AS name,
                payload:username::STRING AS username,
                payload:email::STRING AS email,
                payload:phone::STRING AS phone,
                payload:website::STRING AS website,
                ingested_at AS load_ts,
                filename AS source_file,
                file_row_number,
                ROW_NUMBER() OVER (
                    PARTITION BY payload:id::NUMBER
                    ORDER BY ingested_at DESC, file_row_number DESC, filename DESC
                ) AS rn
            FROM PLATFORM_DB.INGESTION.LANDING_USERS
            WHERE payload:id::NUMBER IS NOT NULL
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
        FROM parsed_landing
        WHERE rn = 1
    ) AS src
    ON tgt.user_id = src.user_id
    WHEN MATCHED THEN UPDATE SET
        tgt.name = src.name,
        tgt.username = src.username,
        tgt.email = src.email,
        tgt.phone = src.phone,
        tgt.website = src.website,
        tgt.load_ts = src.load_ts,
        tgt.source_file = src.source_file
    WHEN NOT MATCHED AND src.user_id IS NOT NULL THEN INSERT (
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

    RETURN 'SP_BACKFILL_USERS completed';
END;
$$;

CREATE OR REPLACE PROCEDURE PLATFORM_DB.PROCESSING.SP_BACKFILL_POSTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    MERGE INTO PLATFORM_DB.PROCESSING.POSTS_TABULAR AS tgt
    USING (
        WITH parsed_landing AS (
            SELECT
                payload:id::NUMBER AS post_id,
                payload:userId::NUMBER AS user_id,
                payload:title::STRING AS title,
                payload:body::STRING AS body,
                ingested_at AS load_ts,
                filename AS source_file,
                file_row_number,
                ROW_NUMBER() OVER (
                    PARTITION BY payload:id::NUMBER
                    ORDER BY ingested_at DESC, file_row_number DESC, filename DESC
                ) AS rn
            FROM PLATFORM_DB.INGESTION.LANDING_POSTS
            WHERE payload:id::NUMBER IS NOT NULL
        )
        SELECT
            post_id,
            user_id,
            title,
            body,
            load_ts,
            source_file
        FROM parsed_landing
        WHERE rn = 1
    ) AS src
    ON tgt.post_id = src.post_id
    WHEN MATCHED THEN UPDATE SET
        tgt.user_id = src.user_id,
        tgt.title = src.title,
        tgt.body = src.body,
        tgt.load_ts = src.load_ts,
        tgt.source_file = src.source_file
    WHEN NOT MATCHED AND src.post_id IS NOT NULL THEN INSERT (
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

    RETURN 'SP_BACKFILL_POSTS completed';
END;
$$;
