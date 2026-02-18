-- Assumed dependencies:
-- - Datavault-UK/automate_dv installed
-- - model ref('stg_dv_posts') available with POST_HK, POST_ID, LOAD_TS, RECORD_SOURCE
-- Execution order:
-- 1) Build stg_dv_posts
-- 2) Build hub_post
-- 3) Build dependent link/sat models
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
{%- set yaml_metadata -%}
source_model: "stg_dv_posts"
src_pk: "POST_HK"
src_nk: "POST_ID"
src_ldts: "LOAD_TS"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(
    src_pk=metadata_dict['src_pk'],
    src_nk=metadata_dict['src_nk'],
    src_ldts=metadata_dict['src_ldts'],
    src_source=metadata_dict['src_source'],
    source_model=metadata_dict['source_model']
) }}
