import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

/**
 * Stock rows may not declare a reorder level above this cap. The invariant is
 * what makes the stock-level report's indexed candidate read complete: any
 * row with `on_hand < reorder_level` also has `on_hand < REORDER_LEVEL_CAP`.
 * See `stockLevelCandidates` in src/warehouse.ts and #1864.
 */
export const REORDER_LEVEL_CAP = 100;

const schema = {
  ...betterAuthSchema,
  warehouses: s
    .table(
      { name: s.string(), region: s.string(), operator_id: s.uuid() },
      {
        districtsViaWarehouse: s.reverse("districts", "warehouse"),
        stockViaWarehouse: s.reverse("stock", "warehouse"),
        customersViaWarehouse: s.reverse("customers", "warehouse"),
        ordersViaWarehouse: s.reverse("orders", "warehouse"),
        order_linesViaWarehouse: s.reverse("order_lines", "warehouse"),
        paymentsViaWarehouse: s.reverse("payments", "warehouse"),
        deliveriesViaWarehouse: s.reverse("deliveries", "warehouse"),
        operatorsViaWarehouse: s.reverse("warehouse_operators", "warehouse"),
      },
    )
    .indexOnly(["operator_id"]),
  // Operators staff one warehouse. `operator_id` on the warehouse remains the
  // manager who may transfer it and staff it; membership rows grant the
  // day-to-day operational writes (checkout, delivery, payment, restock).
  warehouse_operators: s
    .table(
      { warehouse_id: s.uuid(), account_id: s.uuid(), name: s.string() },
      { warehouse: s.rel("warehouses", "warehouse_id") },
    )
    .indexOnly(["warehouse_id", "account_id"]),
  districts: s
    .table(
      {
        warehouse_id: s.uuid(),
        name: s.string(),
        next_order_number: s.int(),
      },
      {
        warehouse: s.rel("warehouses", "warehouse_id"),
        customersViaDistrict: s.reverse("customers", "district"),
        ordersViaDistrict: s.reverse("orders", "district"),
        deliveriesViaDistrict: s.reverse("deliveries", "district"),
      },
    )
    .indexOnly(["warehouse_id", "name"]),
  items: s
    .table(
      {
        sku: s.string(),
        name: s.string(),
        unit_price_cents: s.int(),
        // The global catalogue remains readable to warehouse operators, but its
        // mutable source is still attributable to one operator.
        operator_id: s.uuid(),
      },
      {
        stockViaItem: s.reverse("stock", "item"),
        order_linesViaItem: s.reverse("order_lines", "item"),
      },
    )
    .indexOnly(["operator_id"]),
  stock: s
    .table(
      {
        warehouse_id: s.uuid(),
        item_id: s.uuid(),
        on_hand: s.int(),
        reorder_level: s.int(),
      },
      { warehouse: s.rel("warehouses", "warehouse_id"), item: s.rel("items", "item_id") },
    )
    .indexOnly(["warehouse_id", "item_id", "on_hand"]),
  customers: s
    .table(
      {
        warehouse_id: s.uuid(),
        district_id: s.uuid(),
        name: s.string(),
        balance_cents: s.int(),
      },
      {
        warehouse: s.rel("warehouses", "warehouse_id"),
        district: s.rel("districts", "district_id"),
        ordersViaCustomer: s.reverse("orders", "customer"),
        paymentsViaCustomer: s.reverse("payments", "customer"),
      },
    )
    .indexOnly(["warehouse_id", "district_id", "name"]),
  orders: s
    .table(
      {
        warehouse_id: s.uuid(),
        district_id: s.uuid(),
        customer_id: s.uuid(),
        order_number: s.int(),
        status: s.string(),
        total_cents: s.int(),
        idempotency_key: s.string(),
        // A draft's reservation: the normalised lines and amounts phase one
        // took stock and balance for. Placing or releasing the draft works
        // from this, never from a later request (see `purchase`).
        reserved_lines: s.string().optional(),
      },
      {
        warehouse: s.rel("warehouses", "warehouse_id"),
        district: s.rel("districts", "district_id"),
        customer: s.rel("customers", "customer_id"),
        order_linesViaOrder: s.reverse("order_lines", "order"),
        paymentsViaOrder: s.reverse("payments", "order"),
        deliveriesViaOrder: s.reverse("deliveries", "order"),
      },
    )
    .indexOnly([
      "warehouse_id",
      "district_id",
      "customer_id",
      "status",
      "order_number",
      "idempotency_key",
    ]),
  order_lines: s
    .table(
      {
        warehouse_id: s.uuid(),
        order_id: s.uuid(),
        // TPC-C OL_NUMBER: the line's position on its order, from 1.
        line_number: s.int(),
        item_id: s.uuid(),
        quantity: s.int(),
        amount_cents: s.int(),
      },
      {
        warehouse: s.rel("warehouses", "warehouse_id"),
        order: s.rel("orders", "order_id"),
        item: s.rel("items", "item_id"),
      },
    )
    .indexOnly(["warehouse_id", "order_id"]),
  payments: s
    .table(
      {
        warehouse_id: s.uuid(),
        customer_id: s.uuid(),
        order_id: s.uuid().optional(),
        amount_cents: s.int(),
        idempotency_key: s.string(),
      },
      {
        warehouse: s.rel("warehouses", "warehouse_id"),
        customer: s.rel("customers", "customer_id"),
        order: s.rel("orders", "order_id"),
      },
    )
    .indexOnly(["warehouse_id", "order_id", "idempotency_key"]),
  deliveries: s
    .table(
      {
        warehouse_id: s.uuid(),
        district_id: s.uuid(),
        order_id: s.uuid(),
        status: s.string(),
      },
      {
        warehouse: s.rel("warehouses", "warehouse_id"),
        district: s.rel("districts", "district_id"),
        order: s.rel("orders", "order_id"),
      },
    )
    .indexOnly(["warehouse_id", "order_id"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Warehouse = s.RowOf<typeof app.warehouses>;
export type District = s.RowOf<typeof app.districts>;
export type Item = s.RowOf<typeof app.items>;
export type Stock = s.RowOf<typeof app.stock>;
export type Customer = s.RowOf<typeof app.customers>;
export type Order = s.RowOf<typeof app.orders>;
export type OrderLine = s.RowOf<typeof app.order_lines>;
