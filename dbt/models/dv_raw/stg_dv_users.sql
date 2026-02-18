-- Assumed dependencies:
-- - Datavault-UK/automate_dv installed
-- - model ref('stg_users') available with USER_ID, attributes, LOAD_TS, RECORD_SOURCE
-- Execution order:
-- 1) Build stg_users
-- 2) Build stg_dv_users
-- 3) Build hub/sat models
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
{%- set yaml_metadata -%}
source_model: "stg_users"
hashed_columns:
  USER_HK: "USER_ID"
  USER_HASHDIFF:
    is_hashdiff: true
    columns:
      - "NAME"
      - "USERNAME"
      - "EMAIL"
      - "PHONE"
      - "WEBSITE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    hashed_columns=metadata_dict['hashed_columns']
) }}
