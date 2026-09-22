import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema";

export default definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.always();
  policy.projects.allowInsert.always();
  policy.projects.allowUpdate.always();
  policy.projects.allowDelete.always();

  policy.todos.allowRead.always();
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.always();
  policy.todos.allowDelete.always();
});
