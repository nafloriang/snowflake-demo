-- Assumed dependencies:
-- - Datavault-UK/automate_dv installed
-- - model ref('stg_posts') available with POST_ID, USER_ID, attributes, LOAD_TS, RECORD_SOURCE
-- Execution order:
-- 1) Build stg_posts
-- 2) Build stg_dv_posts
-- 3) Build hub/link/sat models
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
{%- set yaml_metadata -%}
source_model: "stg_posts"
hashed_columns:
  POST_HK: "POST_ID"
  USER_HK: "USER_ID"
  USER_POST_HK:
    - "USER_ID"
    - "POST_ID"
  POST_HASHDIFF:
    is_hashdiff: true
    columns:
      - "TITLE"
      - "BODY"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    hashed_columns=metadata_dict['hashed_columns']
) }}
