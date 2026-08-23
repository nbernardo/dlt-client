const DEFAULT_YAML = `version: 1
tables:
  - table: public.orders as orders
    dimensions:
      - name: id as order_id
      - name: "id + 3"
      - name: "(id + 3) as id_plus_3"
      - name: customer_id
      - name: status as order_status
      - name: "orders.total_revenue - orders.total_discount"
      - name: "orders.total_revenue / NULLIF(orders.distinct_order_count, 0)"

    measures:
      - name: amount as total_revenue
        agg: SUM
      - name: discount_amount as total_discount
        agg: SUM
      - name: id as distinct_order_count
        agg: COUNT_DISTINCT

  - table: public.customers as customers
    dimensions:
      - id as customer_id
      - full_name as customer_name
      - region_id

  - table: public.regions as regions
    dimensions:
      - id as region_id
      - name as region_name

filters:
  - or:
      - "orders.order_status = 'completed'"
      - "orders.order_status = 'shipped'"
  - "regions.region_name = 'North America'"

relationships:
  - name: orders_to_customers
    from_table: orders
    to_table: customers
    join_type: LEFT
    sql_on: "orders.customer_id = customers.customer_id"

  - name: customers_to_regions
    from_table: customers
    to_table: regions
    join_type: LEFT
    sql_on: "customers.region_id = regions.region_id"
`;

const DEFAULT_SCHEMA = {
  'public.orders': ['id', 'customer_id', 'amount', 'discount_amount', 'status', 'created_at','naka_order','naka_name'],
  'public.customers': ['id', 'full_name', 'email', 'region_id'],
  'public.regions': ['id', 'name','region'],
  'public.de_luta': ['eu', 'ele','novoutros'],
};