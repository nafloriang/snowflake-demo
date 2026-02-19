SELECT
    user_id::NUMBER      AS user_id,
    name::STRING         AS name,
    username::STRING     AS username,
    email::STRING        AS email,
    phone::STRING        AS phone,
    website::STRING      AS website,
    load_ts::TIMESTAMP   AS load_ts,
    source_file::STRING  AS record_source
FROM {{ source('processing', 'users_tabular') }}
