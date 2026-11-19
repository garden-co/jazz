import { schema as s } from "jazz-tools";
import { app } from "./schema.js";
export default s.definePermissions(app, ({ policy, session }) => {
  policy.tasks.allowRead.always();
  policy.tasks.allowInsert.where({ owner_id: session.user });
  policy.tasks.allowUpdate.where({ owner_id: session.user });
  policy.tasks.allowDelete.never();
});
