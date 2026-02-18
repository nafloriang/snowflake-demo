-- Assumed dependencies:
-- - source('landing','LANDING_USERS') configured and populated
-- Execution order:
-- 1) Build landing source
-- 2) Build staging view
-- 3) Build dv_raw models
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
SELECT
    payload:id::NUMBER        AS user_id,
    payload:name::STRING      AS name,
    payload:username::STRING  AS username,
    payload:email::STRING     AS email,
    payload:phone::STRING     AS phone,
    payload:website::STRING   AS website,
    ingested_at               AS load_ts,
    'JSONPLACEHOLDER_API'::STRING AS record_source
FROM {{ source('landing', 'LANDING_USERS') }}
