import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({ ownerId: session.user_id });
  policy.todos.allowUpdate
    .whereOld({ ownerId: session.user_id })
    .whereNew({ ownerId: session.user_id });
  policy.todos.allowDelete.where({ ownerId: session.user_id });
});
