import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, allowedTo, session }) => {
  policy.uploads.allowRead.where({ owner_id: session.user_id });
  policy.uploads.allowInsert.where({ owner_id: session.user_id });
  policy.uploads.allowUpdate.never();
  policy.uploads.allowDelete.where({ owner_id: session.user_id });

  // Files are created before the parent upload row exists, so inserts are direct for now.
  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});
  policy.files.allowUpdate.never();
  policy.file_parts.allowUpdate.never();

  policy.files.allowRead.where(allowedTo.readReferencing(policy.uploads, "fileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));

  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.uploads, "fileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
