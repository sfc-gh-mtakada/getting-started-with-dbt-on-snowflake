  -- depends_on: {{ ref('orders') }}

{{ config(
  materialized='semantic_view', 
  tables = [
      {'alias':'orders', 'ref':'orders', 'pk':'order_id'}
  ],
  relationships = []
) }}