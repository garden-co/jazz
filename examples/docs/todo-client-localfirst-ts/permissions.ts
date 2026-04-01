import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({});
  policy.todos.allowUpdate.where({});
  policy.todos.allowDelete.where({});
});
