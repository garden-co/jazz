import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ "$createdBy.account": session.user.account });
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.where({ "$createdBy.account": session.user.account });
  policy.todos.allowDelete.where({ "$createdBy.account": session.user.account });
});
