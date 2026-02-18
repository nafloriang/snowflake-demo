SELECT
  MD5(TO_VARCHAR(id)) AS post_hk,
  title,
  body,
  ingested_at AS load_dts,
  'LANDING_POSTS' AS record_source
FROM {{ ref('stg_posts') }}
WHERE id IS NOT NULL;
