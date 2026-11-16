import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => [
  policy.notes.allowRead.where({ $createdBy: session.user }),
  policy.notes.allowInsert.always(),
  policy.notes.allowUpdate.where({ $createdBy: session.user }),
  policy.notes.allowDelete.where({ $createdBy: session.user }),
]);
