import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate
    .whereOld({ owner_id: session.user_id })
    .whereNew({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });
});
