import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

// Anyone the app's backend admits may read, add and edit every table: this
// example is about streaming an agent's turns, not about access control.
export default definePermissions(app, ({ policy }) => {
  for (const table of [policy.conversations, policy.turns, policy.tool_calls, policy.attachments]) {
    table.allowRead.always();
    table.allowInsert.always();
    table.allowUpdate.always();
  }
});
