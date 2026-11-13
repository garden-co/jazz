import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

// Everyone can read the list. Only a todo's owner can change it.
export default s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.always();
  policy.todos.allowInsert.where({ owner_id: session.user });
  policy.todos.allowUpdate
    .whereOld({ owner_id: session.user })
    .whereNew({ owner_id: session.user });
  policy.todos.allowDelete.where({ owner_id: session.user });
});
