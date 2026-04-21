import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ $createdBy: session.user_id });
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.where({ $createdBy: session.user_id });
  policy.todos.allowDelete.where({ $createdBy: session.user_id });
});
