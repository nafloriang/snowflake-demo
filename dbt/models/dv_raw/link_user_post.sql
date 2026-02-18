-- Assumed dependencies:
-- - Datavault-UK/automate_dv installed
-- - model ref('stg_dv_posts') available with USER_POST_HK, USER_HK, POST_HK, LOAD_TS, RECORD_SOURCE
-- Execution order:
-- 1) Build stg_dv_posts
-- 2) Build hubs
-- 3) Build link_user_post
-- Required dbt commands:
-- - dbt deps
-- - dbt run
-- - dbt test
{%- set yaml_metadata -%}
source_model: "stg_dv_posts"
src_pk: "USER_POST_HK"
src_fk:
  - "USER_HK"
  - "POST_HK"
src_ldts: "LOAD_TS"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(
    src_pk=metadata_dict['src_pk'],
    src_fk=metadata_dict['src_fk'],
    src_ldts=metadata_dict['src_ldts'],
    src_source=metadata_dict['src_source'],
    source_model=metadata_dict['source_model']
) }}
