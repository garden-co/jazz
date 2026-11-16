import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { REORDER_LEVEL_CAP } from "./app-extras";

const { definePermissions, exists, allowedTo, session, anyOf, allOf, gte, lte } = schema;

type TableName = keyof typeof schema.tables & string;
// Distributes over a union, so `Row<"stock" | "orders">` is either row.
type Row<Table extends TableName> = Table extends TableName ? s.Row<typeof schema, Table> : never;

const isWarehouseOperator = (warehouse_id: s.Uuid) =>
  exists.warehouse_operators({ warehouse_id, account_id: session.user });

const managedByMe = { operator_id: session.user };

/**
 * Operational authority for a row's warehouse (#1899): its manager, or an
 * account staffed on that same warehouse. The check follows the row's own
 * `warehouse_id`, so it never trusts a warehouse the row does not belong to.
 */
const operates = (
  row: Row<
    "districts" | "stock" | "customers" | "orders" | "order_lines" | "payments" | "deliveries"
  >,
) => anyOf(allowedTo.update(row.warehouse), isWarehouseOperator(row.warehouse_id));

// Cross-row integrity (#1898): every reference an operational row carries
// must point into the same warehouse (and district) as the row itself, so a
// checkout can never combine rows from two warehouses.

// Stock never goes negative and never declares a reorder level above the
// cap. Checkout relies on the first; the stock-level report on the second.
const validStock = (stock: Row<"stock">) =>
  allOf(
    operates(stock),
    stock.item.where({}),
    allOf(
      { on_hand: gte(0) },
      { reorder_level: gte(0) },
      { reorder_level: lte(REORDER_LEVEL_CAP) },
    ),
  );

const validCustomer = (customer: Row<"customers">) =>
  allOf(operates(customer), customer.district.where({ warehouse_id: customer.warehouse_id }));

const validOrder = (order: Row<"orders">) =>
  allOf(
    operates(order),
    order.district.where({ warehouse_id: order.warehouse_id }),
    order.customer.where({ warehouse_id: order.warehouse_id, district_id: order.district_id }),
  );

// A line belongs to an order of the same warehouse and names an item that
// warehouse stocks.
const validLine = (line: Row<"order_lines">) =>
  allOf(
    operates(line),
    line.order.where({ warehouse_id: line.warehouse_id }),
    exists.stock({ warehouse_id: line.warehouse_id, item_id: line.item_id }),
  );

// A payment is from a customer of the same warehouse, and when it settles an
// order, that order is the same customer's.
const validPayment = (payment: Row<"payments">) =>
  allOf(
    operates(payment),
    payment.customer.where({ warehouse_id: payment.warehouse_id }),
    anyOf(
      { order_id: null },
      payment.order.where({ warehouse_id: payment.warehouse_id, customer_id: payment.customer_id }),
    ),
  );

const validDelivery = (delivery: Row<"deliveries">) =>
  allOf(
    operates(delivery),
    delivery.order.where({
      warehouse_id: delivery.warehouse_id,
      district_id: delivery.district_id,
    }),
  );

export const permissions = definePermissions({
  // Better Auth rows are written by the trusted server route with backend
  // authority. No client session may read or write them.
  better_auth_account: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_jwks: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_session: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_user: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_verification: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  customers: {
    read: true,
    insert: validCustomer,
    updateIf: operates,
    updateTo: validCustomer,
    delete: operates,
  },
  deliveries: {
    read: true,
    insert: validDelivery,
    updateIf: operates,
    updateTo: validDelivery,
    delete: operates,
  },
  districts: {
    read: true,
    insert: operates,
    update: operates,
    delete: operates,
  },
  items: {
    read: true,
    insert: managedByMe,
    update: managedByMe,
    delete: managedByMe,
  },
  order_lines: {
    read: true,
    insert: validLine,
    updateIf: operates,
    updateTo: validLine,
    delete: operates,
  },
  orders: {
    read: true,
    insert: validOrder,
    updateIf: operates,
    updateTo: validOrder,
    delete: operates,
  },
  payments: {
    read: true,
    insert: validPayment,
    updateIf: operates,
    updateTo: validPayment,
    delete: operates,
  },
  stock: {
    read: true,
    insert: validStock,
    updateIf: operates,
    updateTo: validStock,
    delete: operates,
  },
  // Staffing: the manager (or the trusted server bootstrap, which has backend
  // authority) adds operators. An operator may leave; nobody edits a row.
  warehouse_operators: {
    read: true,
    insert: (staffing) => allowedTo.update(staffing.warehouse),
    update: false,
    delete: (staffing) => anyOf(allowedTo.update(staffing.warehouse), { account_id: session.user }),
  },
  // Warehouse metadata stays with its manager. Operational reads are public in
  // this demo so every console can observe stock and queues; reading the
  // manager-only metadata row is too, so the switcher can list warehouses.
  warehouses: {
    read: true,
    insert: managedByMe,
    // Update authority belongs to the current manager. The manager either keeps
    // the warehouse or hands it over to someone already staffed on it; after a
    // handover the former manager is immediately revoked, operational rows
    // included.
    updateIf: managedByMe,
    updateTo: (warehouse) =>
      anyOf(
        managedByMe,
        exists.warehouse_operators({
          warehouse_id: warehouse.id,
          account_id: warehouse.operator_id,
        }),
      ),
  },
});
