SELECT
  payload:id::NUMBER AS id,
  payload:userId::NUMBER AS user_id,
  payload:title::STRING AS title,
  payload:body::STRING AS body,
  ingested_at
FROM {{ source('landing', 'LANDING_POSTS') }};
