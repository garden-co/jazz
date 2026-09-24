import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session, allowedTo }) => {
  policy.warehouses.allowRead.where({ operator_id: session.user.account });
  policy.warehouses.allowInsert.where({ operator_id: session.user.account });
  // Update authority belongs to the current operator. Using the old row is
  // deliberate: it permits an authenticated operator to transfer a warehouse,
  // after which the former operator is immediately revoked.
  policy.warehouses.allowUpdate.whereOld({ operator_id: session.user.account });

  // Operational data is public to the warehouse console, but every mutation
  // must be authorized by the warehouse whose state it changes. `allowedTo`
  // follows the reference rather than trusting an unverified client-supplied
  // warehouse id. Every operational row keeps that authority carrier rather
  // than relying on a newly inserted order/customer to be visible mid-commit.
  policy.districts.allowRead.always();
  policy.districts.allowInsert.where(allowedTo.update("warehouse"));
  policy.districts.allowUpdate
    .whereOld(allowedTo.update("warehouse"))
    .whereNew(allowedTo.update("warehouse"));
  policy.districts.allowDelete.where(allowedTo.update("warehouse"));

  policy.stock.allowRead.always();
  policy.stock.allowInsert.where(allowedTo.update("warehouse"));
  policy.stock.allowUpdate
    .whereOld(allowedTo.update("warehouse"))
    .whereNew(allowedTo.update("warehouse"));
  policy.stock.allowDelete.where(allowedTo.update("warehouse"));

  policy.customers.allowRead.always();
  policy.customers.allowInsert.where(allowedTo.update("warehouse"));
  policy.customers.allowUpdate
    .whereOld(allowedTo.update("warehouse"))
    .whereNew(allowedTo.update("warehouse"));
  policy.customers.allowDelete.where(allowedTo.update("warehouse"));

  policy.orders.allowRead.always();
  policy.orders.allowInsert.where(allowedTo.update("warehouse"));
  policy.orders.allowUpdate
    .whereOld(allowedTo.update("warehouse"))
    .whereNew(allowedTo.update("warehouse"));
  policy.orders.allowDelete.where(allowedTo.update("warehouse"));

  policy.items.allowRead.always();
  policy.items.allowInsert.where({ operator_id: session.user.account });
  policy.items.allowUpdate
    .whereOld({ operator_id: session.user.account })
    .whereNew({ operator_id: session.user.account });
  policy.items.allowDelete.where({ operator_id: session.user.account });

  policy.order_lines.allowRead.always();
  policy.order_lines.allowInsert.where(allowedTo.update("warehouse"));
  policy.order_lines.allowUpdate
    .whereOld(allowedTo.update("warehouse"))
    .whereNew(allowedTo.update("warehouse"));
  policy.order_lines.allowDelete.where(allowedTo.update("warehouse"));

  policy.payments.allowRead.always();
  policy.payments.allowInsert.where(allowedTo.update("warehouse"));
  policy.payments.allowUpdate
    .whereOld(allowedTo.update("warehouse"))
    .whereNew(allowedTo.update("warehouse"));
  policy.payments.allowDelete.where(allowedTo.update("warehouse"));

  policy.deliveries.allowRead.always();
  policy.deliveries.allowInsert.where(allowedTo.update("warehouse"));
  policy.deliveries.allowUpdate
    .whereOld(allowedTo.update("warehouse"))
    .whereNew(allowedTo.update("warehouse"));
  policy.deliveries.allowDelete.where(allowedTo.update("warehouse"));
});
