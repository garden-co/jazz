import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.documents.allowRead.always();
  policy.documents.allowInsert.always();
  policy.documentLogs.allowRead.always();
  policy.documentLogs.allowInsert.always();
  policy.documentLogs.allowUpdate
    .whereOld({ "$createdBy.account": session.user.account })
    .whereNew({ "$createdBy.account": session.user.account });
});
