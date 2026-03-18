import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({ ownerId: session.user_id });
  policy.todos.allowUpdate
    .whereOld({ ownerId: session.user_id })
    .whereNew({ ownerId: session.user_id });
  policy.todos.allowDelete.where({ ownerId: session.user_id });
});
