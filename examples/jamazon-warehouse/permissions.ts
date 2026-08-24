import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.warehouses.allowRead.where({ operator_id: session.user_id });
  policy.warehouses.allowInsert.where({ operator_id: session.user_id });
  policy.warehouses.allowUpdate.where({ operator_id: session.user_id });
  for (const table of [
    policy.districts,
    policy.items,
    policy.stock,
    policy.customers,
    policy.orders,
    policy.order_lines,
    policy.payments,
    policy.deliveries,
  ]) {
    table.allowRead.always();
    table.allowInsert.always();
    table.allowUpdate.always();
  }
});
