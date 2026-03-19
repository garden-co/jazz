import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

// #region permissions-basic-vue
export default definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.where({}),
  policy.todos.allowInsert.where({ done: false }),
  policy.todos.allowUpdate.whereOld({ done: false }).whereNew({}),
  policy.todos.allowDelete.where({ done: false }),
]);
// #endregion permissions-basic-vue
