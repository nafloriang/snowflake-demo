CREATE OR REPLACE PROCEDURE PLATFORM_DB.PROCESSING.SP_PROCESS_USERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    -- Consume only INSERT records from stream, ignore update artifacts,
    -- parse JSON safely, and deduplicate deterministically by user_id.
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
