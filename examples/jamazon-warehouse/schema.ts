import { schema as s } from "jazz-tools";

const schema = {
  warehouses: s.table({ name: s.string(), region: s.string(), operator_id: s.string() }),
  districts: s.table({
    warehouse_id: s.ref("warehouses"),
    name: s.string(),
    next_order_number: s.int(),
  }),
  items: s.table({ sku: s.string(), name: s.string(), unit_price_cents: s.int() }),
  stock: s.table({
    warehouse_id: s.ref("warehouses"),
    item_id: s.ref("items"),
    on_hand: s.int(),
    reorder_level: s.int(),
  }),
  customers: s.table({
    warehouse_id: s.ref("warehouses"),
    district_id: s.ref("districts"),
    name: s.string(),
    balance_cents: s.int(),
  }),
  orders: s.table({
    warehouse_id: s.ref("warehouses"),
    district_id: s.ref("districts"),
    customer_id: s.ref("customers"),
    order_number: s.int(),
    status: s.string(),
    total_cents: s.int(),
    idempotency_key: s.string(),
  }),
  order_lines: s.table({
    order_id: s.ref("orders"),
    item_id: s.ref("items"),
    quantity: s.int(),
    amount_cents: s.int(),
  }),
  payments: s.table({
    customer_id: s.ref("customers"),
    order_id: s.ref("orders").optional(),
    amount_cents: s.int(),
    idempotency_key: s.string(),
  }),
  deliveries: s.table({
    warehouse_id: s.ref("warehouses"),
    district_id: s.ref("districts"),
    order_id: s.ref("orders"),
    status: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
