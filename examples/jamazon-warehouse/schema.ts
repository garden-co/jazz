import { schema as s } from "jazz-tools";

const schema = {
  warehouses: s
    .table({ name: s.string(), region: s.string(), operator_id: s.string() })
    .indexOnly(["operator_id"]),
  districts: s
    .table({
      warehouse_id: s.ref("warehouses"),
      name: s.string(),
      next_order_number: s.int(),
    })
    .indexOnly(["warehouse_id", "name"]),
  items: s
    .table({
      sku: s.string(),
      name: s.string(),
      unit_price_cents: s.int(),
      // The global catalogue remains readable to warehouse operators, but its
      // mutable source is still attributable to one operator.
      operator_id: s.string(),
    })
    .indexOnly(["operator_id"]),
  stock: s
    .table({
      warehouse_id: s.ref("warehouses"),
      item_id: s.ref("items"),
      on_hand: s.int(),
      reorder_level: s.int(),
    })
    .indexOnly(["warehouse_id", "item_id", "on_hand"]),
  customers: s
    .table({
      warehouse_id: s.ref("warehouses"),
      district_id: s.ref("districts"),
      name: s.string(),
      balance_cents: s.int(),
    })
    .indexOnly(["warehouse_id", "district_id", "name"]),
  orders: s
    .table({
      warehouse_id: s.ref("warehouses"),
      district_id: s.ref("districts"),
      customer_id: s.ref("customers"),
      order_number: s.int(),
      status: s.string(),
      total_cents: s.int(),
      idempotency_key: s.string(),
    })
    .indexOnly(["warehouse_id", "district_id", "status", "order_number", "idempotency_key"]),
  order_lines: s
    .table({
      warehouse_id: s.ref("warehouses"),
      order_id: s.ref("orders"),
      item_id: s.ref("items"),
      quantity: s.int(),
      amount_cents: s.int(),
    })
    .indexOnly(["warehouse_id", "order_id"]),
  payments: s
    .table({
      warehouse_id: s.ref("warehouses"),
      customer_id: s.ref("customers"),
      order_id: s.ref("orders").optional(),
      amount_cents: s.int(),
      idempotency_key: s.string(),
    })
    .indexOnly(["warehouse_id", "order_id"]),
  deliveries: s.table({
    warehouse_id: s.ref("warehouses"),
    district_id: s.ref("districts"),
    order_id: s.ref("orders"),
    status: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
