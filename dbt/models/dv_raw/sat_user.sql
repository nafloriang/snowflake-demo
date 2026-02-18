SELECT
  MD5(TO_VARCHAR(id)) AS user_hk,
  name,
  username,
  email,
  phone,
  website,
  ingested_at AS load_dts,
  'LANDING_USERS' AS record_source
FROM {{ ref('stg_users') }}
WHERE id IS NOT NULL;
