import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

// #region files-permissions-ts
export const fileBlobPermissions = s.definePermissions(app, ({ policy, allowedTo, session }) => {
  policy.uploads.allowRead.where({ owner_id: session.user_id });
  policy.uploads.allowInsert.where({ owner_id: session.user_id });
  policy.uploads.allowUpdate.where({ owner_id: session.user_id });
  policy.uploads.allowDelete.where({ owner_id: session.user_id });

  // Files are created before the parent upload row exists, so inserts are direct for now.
  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});

  policy.files.allowRead.where(allowedTo.readReferencing(policy.uploads, "fileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));

  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.uploads, "fileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
// #endregion files-permissions-ts
