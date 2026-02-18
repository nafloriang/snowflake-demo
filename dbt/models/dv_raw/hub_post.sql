SELECT DISTINCT
  MD5(TO_VARCHAR(id)) AS post_hk,
  id AS post_id,
  ingested_at AS load_dts,
  'LANDING_POSTS' AS record_source
FROM {{ ref('stg_posts') }}
WHERE id IS NOT NULL;
