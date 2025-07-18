-- depends_on: {{ ref('test_semantic') }}  -- ← Semantic View を静的に参照

{{ config(
    materialized = 'semantic_file',
    view_fqn     = 'TASTY_BYTES_DBT_DB.DEV.TEST_SEMANTIC',
    stage_fqn    = 'TASTY_BYTES_DBT_DB.DEV.YAML_STG',
    file_name    = 'test_semantic.yaml'
) }}