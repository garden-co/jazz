import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.where({}),
  policy.todos.allowInsert.where({}),
  policy.todos.allowUpdate.where({}),
  policy.todos.allowDelete.where({}),
]);
