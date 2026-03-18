import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

// #region files-permissions-ts
export const fileBlobPermissions = definePermissions(app, ({ policy, allowedTo, session }) => {
  policy.uploads.allowRead.where({ ownerId: session.user_id });
  policy.uploads.allowInsert.where({ ownerId: session.user_id });
  policy.uploads.allowUpdate.where({ ownerId: session.user_id });
  policy.uploads.allowDelete.where({ ownerId: session.user_id });

  // Files are created before the parent upload row exists, so inserts are direct for now.
  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});

  policy.files.allowRead.where(allowedTo.readReferencing(policy.uploads, "fileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));

  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.uploads, "fileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
// #endregion files-permissions-ts
