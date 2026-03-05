import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy }) => {
  policy.counter_events.allowRead.where({});
  policy.counter_events.allowInsert.where({});
});
