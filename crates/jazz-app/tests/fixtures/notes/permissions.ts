import { schema as s } from "jazz-tools";
import { app } from "./schema.ts";
export default s.definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ $createdBy: session.user });
  policy.notes.allowInsert.always();
});
