-- Assumed dependencies:
-- - Datavault-UK/automate_dv installed
-- - model ref('stg_dv_users') available with USER_HK, USER_HASHDIFF, descriptive columns, LOAD_TS, RECORD_SOURCE
-- Execution order:
-- 1) Build stg_dv_users
-- 2) Build hub_user
-- 3) Build sat_user
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
{%- set yaml_metadata -%}
source_model: "stg_dv_users"
src_pk: "USER_HK"
src_hashdiff: "USER_HASHDIFF"
src_payload:
  - "NAME"
  - "USERNAME"
  - "EMAIL"
  - "PHONE"
  - "WEBSITE"
src_ldts: "LOAD_TS"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(
    src_pk=metadata_dict['src_pk'],
    src_hashdiff=metadata_dict['src_hashdiff'],
    src_payload=metadata_dict['src_payload'],
    src_ldts=metadata_dict['src_ldts'],
    src_source=metadata_dict['src_source'],
    source_model=metadata_dict['source_model']
) }}
