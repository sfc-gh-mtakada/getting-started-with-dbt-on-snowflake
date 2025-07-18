-- depends_on: {{ ref('test') }}
-- depends_on: {{ ref('test2') }}

{{ config(
    materialized = 'semantic_view',
    tables = [
      {'alias':'test', 'ref':'test', 'pk':'id'},
      {'alias':'test2', 'ref':'test2', 'pk':'id'},
    ],
    relationships = [{'left':'test', 'left_cols':['id'], 'right':'test2', 'right_cols':['id']}] 
) }}