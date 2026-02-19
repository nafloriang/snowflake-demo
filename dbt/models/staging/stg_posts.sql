SELECT
    post_id::NUMBER      AS post_id,
    user_id::NUMBER      AS user_id,
    title::STRING        AS title,
    body::STRING         AS body,
    load_ts::TIMESTAMP   AS load_ts,
    source_file::STRING  AS record_source
FROM {{ source('processing', 'posts_tabular') }}
