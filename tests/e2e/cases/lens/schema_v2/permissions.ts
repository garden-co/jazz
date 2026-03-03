import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({});
});
