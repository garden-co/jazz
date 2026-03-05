import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy }) => {
  policy.users.allowRead.where({});
  policy.users.allowInsert.where({});
  policy.canvases.allowRead.where({});
  policy.canvases.allowInsert.where({});
  policy.strokes.allowRead.where({});
  policy.strokes.allowInsert.where({});
});
