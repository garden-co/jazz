import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

// Foundation-only private folders. Shared-folder membership and revocation need
// their own application schema and product contract.
export default s.definePermissions(app, ({ allOf, allowedTo, policy, session }) => {
  policy.folders.allowRead.where({ owner_id: session.user.account });
  policy.folders.allowInsert.where({ owner_id: session.user.account });
  policy.folders.allowUpdate
    .whereOld({ owner_id: session.user.account })
    .whereNew({ owner_id: session.user.account });
  policy.folders.allowDelete.where({ owner_id: session.user.account });

  // File ownership alone must not let a caller attach it to (or move it into)
  // another account's private folder. Keep the child and referenced-parent
  // authority checks paired for every operation.
  policy.files.allowRead.where((_file) =>
    allOf([{ owner_id: session.user.account }, allowedTo.read("folder_id")]),
  );
  policy.files.allowInsert.where((_file) =>
    allOf([{ owner_id: session.user.account }, allowedTo.insert("folder_id")]),
  );
  policy.files.allowUpdate
    .whereOld((_file) => allOf([{ owner_id: session.user.account }, allowedTo.update("folder_id")]))
    .whereNew((_file) =>
      allOf([{ owner_id: session.user.account }, allowedTo.update("folder_id")]),
    );
  policy.files.allowDelete.where((_file) =>
    allOf([{ owner_id: session.user.account }, allowedTo.delete("folder_id")]),
  );
});
