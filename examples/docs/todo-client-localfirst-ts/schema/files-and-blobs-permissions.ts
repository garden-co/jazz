import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

// #region files-permissions-ts
export const fileBlobPermissions = definePermissions(app, ({ policy, allowedTo, session }) => {
  policy.uploads.allowRead.where({ owner_id: session.user_id });
  policy.uploads.allowInsert.where({ owner_id: session.user_id });
  policy.uploads.allowUpdate.where({ owner_id: session.user_id });
  policy.uploads.allowDelete.where({ owner_id: session.user_id });

  // Files are created before the parent upload row exists, so inserts are direct for now.
  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});

  policy.files.allowRead.where(allowedTo.readReferencing(policy.uploads, "file"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "parts"));

  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.uploads, "file"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "parts"));
});
// #endregion files-permissions-ts
