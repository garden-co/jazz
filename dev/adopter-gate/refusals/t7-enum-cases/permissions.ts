import { schema as s } from "jazz-tools";
import { app } from "./schema.js";
export default s.definePermissions(app, ({ policy, session }) => {
  policy.tickets.allowRead.where({ owner_id: session.user }); policy.tickets.allowInsert.where({ owner_id: session.user });
  policy.tickets.allowUpdate.never(); policy.tickets.allowDelete.never();
});
