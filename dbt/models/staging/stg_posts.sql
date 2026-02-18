-- Assumed dependencies:
-- - source('landing','LANDING_POSTS') configured and populated
-- Execution order:
-- 1) Build landing source
-- 2) Build staging view
-- 3) Build dv_raw models
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
SELECT
    payload:id::NUMBER        AS post_id,
    payload:userId::NUMBER    AS user_id,
    payload:title::STRING     AS title,
    payload:body::STRING      AS body,
    ingested_at               AS load_ts,
    'JSONPLACEHOLDER_API'::STRING AS record_source
FROM {{ source('landing', 'LANDING_POSTS') }}
