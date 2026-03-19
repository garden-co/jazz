import { definePermissions } from "../../../../src/permissions/index.js";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({});
  policy.todos.allowUpdate.whereOld({ done: false }).whereNew({});
  policy.todos.allowDelete.where({ done: false });
});
