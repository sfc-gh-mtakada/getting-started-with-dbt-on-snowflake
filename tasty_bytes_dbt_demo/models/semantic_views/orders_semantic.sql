{{ config(materialized='semantic_view') }}

TABLES (
  orders AS {{ ref('orders') }} PRIMARY KEY (order_id)
)

FACTS (
  orders.order_total AS orders.order_total 
    COMMENT = 'Order total (inc. tax/discount)'
)

DIMENSIONS (
  orders.truck_id AS orders.truck_id
    COMMENT = 'Truck ID',
  orders.menu_type AS orders.menu_type
    COMMENT = 'Menu Category',
  orders.order_ts AS orders.order_ts 
    COMMENT = 'Order timestamp'
)

METRICS (
  orders.order_count AS COUNT(orders.order_id),
  orders.gross_sales AS SUM(orders.order_amount)
)