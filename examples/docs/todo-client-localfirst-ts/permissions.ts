// #region quickstart-permissions-ts
import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy }) => {
  policy.projects.allowRead.always();
  policy.projects.allowInsert.always();

  policy.todos.allowRead.always();
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.always();
  policy.todos.allowDelete.always();
});
// #endregion quickstart-permissions-ts
