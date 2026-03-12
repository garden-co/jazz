import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, session }) => {
  // Products: globally readable (catalog)
  policy.products.allowRead.where({});

  // Cart items: owner-scoped CRUD
  policy.cart_items.allowRead.where({ owner_id: session.user_id });
  policy.cart_items.allowInsert.where({ owner_id: session.user_id });
  policy.cart_items.allowUpdate.where({ owner_id: session.user_id });
  policy.cart_items.allowDelete.where({ owner_id: session.user_id });

  // Orders: owner can read own orders (server creates them via admin)
  policy.orders.allowRead.where({ owner_id: session.user_id });

  // Order items: globally readable (server creates them)
  policy.order_items.allowRead.where({});
});
