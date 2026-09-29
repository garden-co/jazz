import { schema as s } from "jazz-tools";
import type { RowContext } from "jazz-tools/permissions";
import { app, REORDER_LEVEL_CAP } from "./schema";

/** A policy row context with (at least) the named columns. */
type Row<Column extends string> = RowContext<Record<Column, unknown>>;

export default s.definePermissions(app, ({ policy, session, allowedTo, anyOf, allOf }) => {
  // Better Auth rows are written by the trusted server route with backend
  // authority. No client session may read or write them.
  for (const table of [
    policy.better_auth_user,
    policy.better_auth_session,
    policy.better_auth_account,
    policy.better_auth_verification,
    policy.better_auth_jwks,
  ]) {
    table.allowRead.never();
    table.allowInsert.never();
    table.allowUpdate.never();
    table.allowDelete.never();
  }

  // Warehouse metadata stays with its manager. Operational reads are public in
  // this demo so every console can observe stock and queues; reading the
  // manager-only metadata row is too, so the switcher can list warehouses.
  policy.warehouses.allowRead.always();
  policy.warehouses.allowInsert.where({ operator_id: session.user.account });
  // Update authority belongs to the current manager. The new row is
  // unconstrained on purpose: a manager may transfer the warehouse, after
  // which the former manager is immediately revoked, operational rows included.
  // (`whereOld` alone would apply the same condition to the new row too.)
  policy.warehouses.allowUpdate.whereOld({ operator_id: session.user.account }).whereNew({});

  // Staffing: the manager (or the trusted server bootstrap, which has backend
  // authority) adds operators. An operator may leave; nobody edits a row.
  policy.warehouse_operators.allowRead.always();
  policy.warehouse_operators.allowInsert.where(allowedTo.update("warehouse"));
  policy.warehouse_operators.allowUpdate.never();
  policy.warehouse_operators.allowDelete.where(
    anyOf([allowedTo.update("warehouse"), { account_id: session.user.account }]),
  );

  /**
   * Operational authority for a row's warehouse (#1899): its manager, or an
   * account staffed on that same warehouse. The check follows the row's own
   * `warehouse_id`, so it never trusts a warehouse the row does not belong to.
   */
  const operates = (row: Row<"warehouse_id">) =>
    anyOf([
      allowedTo.update("warehouse"),
      policy.warehouse_operators.exists.where({
        warehouse_id: row.warehouse_id,
        account_id: session.user.account,
      }),
    ]);

  // Cross-row integrity (#1898): every reference an operational row carries
  // must point into the same warehouse (and district) as the row itself, so a
  // checkout can never combine rows from two warehouses.
  const districtInWarehouse = (row: Row<"warehouse_id" | "district_id">) =>
    policy.districts.exists.where({ id: row.district_id, warehouse_id: row.warehouse_id });
  const customerInDistrict = (row: Row<"warehouse_id" | "district_id" | "customer_id">) =>
    policy.customers.exists.where({
      id: row.customer_id,
      warehouse_id: row.warehouse_id,
      district_id: row.district_id,
    });

  policy.districts.allowRead.always();
  policy.districts.allowInsert.where((district) => operates(district));
  policy.districts.allowUpdate
    .whereOld((district) => operates(district))
    .whereNew((district) => operates(district));
  policy.districts.allowDelete.where((district) => operates(district));

  // Stock never goes negative and never declares a reorder level above the
  // cap. Checkout relies on the first; the stock-level report on the second.
  const validStock = (stock: Row<"warehouse_id" | "item_id">) =>
    allOf([
      operates(stock),
      policy.items.exists.where({ id: stock.item_id }),
      { on_hand: { gte: 0 }, reorder_level: { gte: 0, lte: REORDER_LEVEL_CAP } },
    ]);
  policy.stock.allowRead.always();
  policy.stock.allowInsert.where((stock) => validStock(stock));
  policy.stock.allowUpdate
    .whereOld((stock) => operates(stock))
    .whereNew((stock) => validStock(stock));
  policy.stock.allowDelete.where((stock) => operates(stock));

  policy.customers.allowRead.always();
  policy.customers.allowInsert.where((customer) =>
    allOf([operates(customer), districtInWarehouse(customer)]),
  );
  policy.customers.allowUpdate
    .whereOld((customer) => operates(customer))
    .whereNew((customer) => allOf([operates(customer), districtInWarehouse(customer)]));
  policy.customers.allowDelete.where((customer) => operates(customer));

  const validOrder = (order: Row<"warehouse_id" | "district_id" | "customer_id">) =>
    allOf([operates(order), districtInWarehouse(order), customerInDistrict(order)]);
  policy.orders.allowRead.always();
  policy.orders.allowInsert.where((order) => validOrder(order));
  policy.orders.allowUpdate
    .whereOld((order) => operates(order))
    .whereNew((order) => validOrder(order));
  policy.orders.allowDelete.where((order) => operates(order));

  policy.items.allowRead.always();
  policy.items.allowInsert.where({ operator_id: session.user.account });
  policy.items.allowUpdate
    .whereOld({ operator_id: session.user.account })
    .whereNew({ operator_id: session.user.account });
  policy.items.allowDelete.where({ operator_id: session.user.account });

  // A line belongs to an order of the same warehouse and names an item that
  // warehouse stocks.
  const validLine = (line: Row<"warehouse_id" | "order_id" | "item_id">) =>
    allOf([
      operates(line),
      policy.orders.exists.where({ id: line.order_id, warehouse_id: line.warehouse_id }),
      policy.stock.exists.where({ warehouse_id: line.warehouse_id, item_id: line.item_id }),
    ]);
  policy.order_lines.allowRead.always();
  policy.order_lines.allowInsert.where((line) => validLine(line));
  policy.order_lines.allowUpdate
    .whereOld((line) => operates(line))
    .whereNew((line) => validLine(line));
  policy.order_lines.allowDelete.where((line) => operates(line));

  // A payment is from a customer of the same warehouse, and when it settles an
  // order, that order is the same customer's.
  const validPayment = (payment: Row<"warehouse_id" | "customer_id" | "order_id">) =>
    allOf([
      operates(payment),
      policy.customers.exists.where({
        id: payment.customer_id,
        warehouse_id: payment.warehouse_id,
      }),
      anyOf([
        { order_id: { isNull: true } },
        policy.orders.exists.where({
          id: payment.order_id,
          warehouse_id: payment.warehouse_id,
          customer_id: payment.customer_id,
        }),
      ]),
    ]);
  policy.payments.allowRead.always();
  policy.payments.allowInsert.where((payment) => validPayment(payment));
  policy.payments.allowUpdate
    .whereOld((payment) => operates(payment))
    .whereNew((payment) => validPayment(payment));
  policy.payments.allowDelete.where((payment) => operates(payment));

  const validDelivery = (delivery: Row<"warehouse_id" | "district_id" | "order_id">) =>
    allOf([
      operates(delivery),
      policy.orders.exists.where({
        id: delivery.order_id,
        warehouse_id: delivery.warehouse_id,
        district_id: delivery.district_id,
      }),
    ]);
  policy.deliveries.allowRead.always();
  policy.deliveries.allowInsert.where((delivery) => validDelivery(delivery));
  policy.deliveries.allowUpdate
    .whereOld((delivery) => operates(delivery))
    .whereNew((delivery) => validDelivery(delivery));
  policy.deliveries.allowDelete.where((delivery) => operates(delivery));
});
