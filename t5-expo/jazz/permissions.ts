import { schema as s } from "jazz-tools";
import { app } from "./schema.js";
export default s.definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ owner_id: session.user }); policy.notes.allowInsert.where({ owner_id: session.user });
  policy.notes.allowUpdate.where({ owner_id: session.user }); policy.notes.allowDelete.where({ owner_id: session.user });
});
