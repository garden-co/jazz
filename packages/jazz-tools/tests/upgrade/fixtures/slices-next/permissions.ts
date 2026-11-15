import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema";

export default definePermissions(app, ({ policy }) => [
  policy.boards.allowRead.always(),
  policy.cards.allowRead.always(),
  policy.cards.allowInsert.always(),
]);
