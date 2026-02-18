SELECT DISTINCT
  MD5(CONCAT(TO_VARCHAR(user_id), '|', TO_VARCHAR(id))) AS user_post_hk,
  MD5(TO_VARCHAR(user_id)) AS user_hk,
  MD5(TO_VARCHAR(id)) AS post_hk,
  user_id,
  id AS post_id,
  ingested_at AS load_dts,
  'LANDING_POSTS' AS record_source
FROM {{ ref('stg_posts') }}
WHERE user_id IS NOT NULL
  AND id IS NOT NULL;
