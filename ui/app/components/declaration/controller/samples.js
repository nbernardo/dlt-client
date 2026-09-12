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



export const sampleRules = [
    { id: "dq_101", type: "NOT_NULL", column: "order_id", severity: "ERROR", params: {} },
    { id: "dq_102", type: "UNIQUE", column: "order_id", severity: "CRITICAL", params: {} },
    { id: "dq_103", type: "ACCEPTED_VALUES", column: "order_status", severity: "WARN", params: { values: "'completed', 'shipped', 'pending', 'cancelled'" } },
    { id: "dq_104", type: "VALUE_RANGE", column: "total_amount", severity: "ERROR", params: { min: "0.00", max: "100000.00" } },
    { id: "dq_105", type: "CUSTOM_SQL", column: "", severity: "ERROR", params: { sql: "discount_amount <= total_amount" } },
    { id: "dq_106", type: "REFERENTIAL_INTEGRITY", column: "customer_id", severity: "ERROR", params: { ref_table: "public.customers", ref_column: "customer_id" } },
    { id: "dq_107", type: "REGEX_MATCH", column: "customer_email", severity: "WARN", params: { pattern: "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$" } },
    { id: "dq_108", type: "FRESHNESS", column: "updated_at", severity: "WARN", params: { max_age_hours: "24" } },
    { id: "dq_109", type: "ROW_COUNT", column: "", severity: "CRITICAL", params: { min_rows: "1", max_rows: "" } }
];


export const sampleTables = {
    'public.orders': ['order_id', 'order_status', 'total_amount', 'discount_amount', 'customer_id', 'customer_email', 'updated_at'],
    'public.customers': ['customer_id', 'name', 'email', 'created_at', 'updated_at'],
    'public.products': ['product_id', 'sku', 'name', 'price', 'category', 'in_stock'],
    'public.payments': ['payment_id', 'order_id', 'amount', 'status', 'processed_at']
}