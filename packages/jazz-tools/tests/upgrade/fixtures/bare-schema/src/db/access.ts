import { definePermissions } from "jazz-tools/permissions";
import { app } from "./index.js";
export default definePermissions(app, ({ policy, session }) => {
  policy.boards.allowRead.where({ owner_id: session.user });
  policy.boards.allowInsert.where({ owner_id: session.user });
  policy.boards.allowUpdate.never();
  policy.boards.allowDelete.never();
  policy.notes.allowRead.where({ owner_id: session.user });
  policy.notes.allowInsert.where({ owner_id: session.user });
  policy.notes.allowUpdate.where({ owner_id: session.user });
  policy.notes.allowDelete.where({ owner_id: session.user });
});
