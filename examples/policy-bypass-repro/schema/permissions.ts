import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, session }) => {
  policy.owned_items.allowRead.where({ ownerId: session.user_id });
});
