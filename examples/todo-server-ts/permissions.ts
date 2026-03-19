import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ ownerId: session.user_id });
  policy.todos.allowInsert.where({ ownerId: session.user_id });
  policy.todos.allowUpdate.where({ ownerId: session.user_id });
  policy.todos.allowDelete.where({ ownerId: session.user_id });
});
