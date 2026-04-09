import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.always(),
  policy.projects.allowDelete.always(),
  policy.todos.allowRead.where({}),
  policy.todos.allowInsert.where({}),
  policy.todos.allowUpdate.where({}),
  policy.todos.allowDelete.where({}),
]);
