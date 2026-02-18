SELECT DISTINCT
  MD5(TO_VARCHAR(id)) AS user_hk,
  id AS user_id,
  ingested_at AS load_dts,
  'LANDING_USERS' AS record_source
FROM {{ ref('stg_users') }}
WHERE id IS NOT NULL;
