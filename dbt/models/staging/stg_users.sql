SELECT
  payload:id::NUMBER AS id,
  payload:name::STRING AS name,
  payload:username::STRING AS username,
  payload:email::STRING AS email,
  payload:phone::STRING AS phone,
  payload:website::STRING AS website,
  ingested_at
FROM {{ source('landing', 'LANDING_USERS') }};
