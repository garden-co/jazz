import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.always();
  policy.projects.allowInsert.always();

  policy.todos.allowRead.where({ owner_id: session.user });
  policy.todos.allowInsert.where({ owner_id: session.user });
  policy.todos.allowUpdate.where({ owner_id: session.user });
  policy.todos.allowDelete.where({ owner_id: session.user });
});
