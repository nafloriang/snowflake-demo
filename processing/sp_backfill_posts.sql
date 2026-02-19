CREATE OR REPLACE PROCEDURE PLATFORM_DB.PROCESSING.SP_BACKFILL_POSTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    -- Reprocess full post history from landing table (not stream),
    -- then upsert latest deterministic record per post_id.
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
