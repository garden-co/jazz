import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema/app.js";

// All three tables are shared world state — every authenticated player can
// read and write. Identity is used for filtering (e.g. whose row is whose)
// not for access control.

export default definePermissions(app, ({ policy }) => {
  policy.players.allowRead.where({});
  policy.players.allowInsert.where({});
  policy.players.allowUpdate.whereOld({}).whereNew({});
  policy.players.allowDelete.where({});

  policy.fuel_deposits.allowRead.where({});
  policy.fuel_deposits.allowInsert.where({});
  policy.fuel_deposits.allowUpdate.whereOld({}).whereNew({});
  policy.fuel_deposits.allowDelete.where({});

  policy.chat_messages.allowRead.where({});
  policy.chat_messages.allowInsert.where({});
});
