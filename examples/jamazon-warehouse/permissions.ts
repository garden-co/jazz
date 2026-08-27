import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session, allowedTo }) => {
  policy.warehouses.allowRead.where({ operator_id: session.user });
  policy.warehouses.allowInsert.where({ operator_id: session.user });
  // Update authority belongs to the current operator. Using the old row is
  // deliberate: it permits an authenticated operator to transfer a warehouse,
  // after which the former operator is immediately revoked.
  policy.warehouses.allowUpdate.whereOld({ operator_id: session.user });

  // Operational data is public to the warehouse console, but every mutation
  // must be authorized by the warehouse whose state it changes. `allowedTo`
  // follows the reference rather than trusting an unverified client-supplied
  // warehouse id. Every operational row keeps that authority carrier rather
  // than relying on a newly inserted order/customer to be visible mid-commit.
  policy.districts.allowRead.always();
  policy.districts.allowInsert.where(allowedTo.update("warehouse_id"));
  policy.districts.allowUpdate
    .whereOld(allowedTo.update("warehouse_id"))
    .whereNew(allowedTo.update("warehouse_id"));
  policy.districts.allowDelete.where(allowedTo.update("warehouse_id"));

  policy.stock.allowRead.always();
  policy.stock.allowInsert.where(allowedTo.update("warehouse_id"));
  policy.stock.allowUpdate
    .whereOld(allowedTo.update("warehouse_id"))
    .whereNew(allowedTo.update("warehouse_id"));
  policy.stock.allowDelete.where(allowedTo.update("warehouse_id"));

  policy.customers.allowRead.always();
  policy.customers.allowInsert.where(allowedTo.update("warehouse_id"));
  policy.customers.allowUpdate
    .whereOld(allowedTo.update("warehouse_id"))
    .whereNew(allowedTo.update("warehouse_id"));
  policy.customers.allowDelete.where(allowedTo.update("warehouse_id"));

  policy.orders.allowRead.always();
  policy.orders.allowInsert.where(allowedTo.update("warehouse_id"));
  policy.orders.allowUpdate
    .whereOld(allowedTo.update("warehouse_id"))
    .whereNew(allowedTo.update("warehouse_id"));
  policy.orders.allowDelete.where(allowedTo.update("warehouse_id"));

  policy.items.allowRead.always();
  policy.items.allowInsert.where({ operator_id: session.user });
  policy.items.allowUpdate
    .whereOld({ operator_id: session.user })
    .whereNew({ operator_id: session.user });
  policy.items.allowDelete.where({ operator_id: session.user });

  policy.order_lines.allowRead.always();
  policy.order_lines.allowInsert.where(allowedTo.update("warehouse_id"));
  policy.order_lines.allowUpdate
    .whereOld(allowedTo.update("warehouse_id"))
    .whereNew(allowedTo.update("warehouse_id"));
  policy.order_lines.allowDelete.where(allowedTo.update("warehouse_id"));

  policy.payments.allowRead.always();
  policy.payments.allowInsert.where(allowedTo.update("warehouse_id"));
  policy.payments.allowUpdate
    .whereOld(allowedTo.update("warehouse_id"))
    .whereNew(allowedTo.update("warehouse_id"));
  policy.payments.allowDelete.where(allowedTo.update("warehouse_id"));

  policy.deliveries.allowRead.always();
  policy.deliveries.allowInsert.where(allowedTo.update("warehouse_id"));
  policy.deliveries.allowUpdate
    .whereOld(allowedTo.update("warehouse_id"))
    .whereNew(allowedTo.update("warehouse_id"));
  policy.deliveries.allowDelete.where(allowedTo.update("warehouse_id"));
});
