import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

// #region permissions-basic-react
export default definePermissions(app, ({ policy }) => [
  // Everyone can read todos.
  policy.todos.allowRead.where({}),
  // New todos start as incomplete.
  policy.todos.allowInsert.where({ done: false }),
  // Completed todos are immutable.
  policy.todos.allowUpdate.whereOld({ done: false }).whereNew({}),
  // Only open todos can be deleted.
  policy.todos.allowDelete.where({ done: false }),
]);
// #endregion permissions-basic-react
