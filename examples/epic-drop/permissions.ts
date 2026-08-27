import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

// Foundation-only private folders. Shared-folder membership and revocation need
// their own application schema and product contract.
export default s.definePermissions(app, ({ allOf, allowedTo, policy, session }) => {
  policy.folders.allowRead.where({ owner_id: session.user });
  policy.folders.allowInsert.where({ owner_id: session.user });
  policy.folders.allowUpdate
    .whereOld({ owner_id: session.user })
    .whereNew({ owner_id: session.user });
  policy.folders.allowDelete.where({ owner_id: session.user });

  // File ownership alone must not let a caller attach it to (or move it into)
  // another principal's private folder. Keep the child and referenced-parent
  // authority checks paired for every operation.
  policy.files.allowRead.where((_file) =>
    allOf([{ owner_id: session.user }, allowedTo.read("folder_id")]),
  );
  policy.files.allowInsert.where((_file) =>
    allOf([{ owner_id: session.user }, allowedTo.insert("folder_id")]),
  );
  policy.files.allowUpdate
    .whereOld((_file) => allOf([{ owner_id: session.user }, allowedTo.update("folder_id")]))
    .whereNew((_file) => allOf([{ owner_id: session.user }, allowedTo.update("folder_id")]));
  policy.files.allowDelete.where((_file) =>
    allOf([{ owner_id: session.user }, allowedTo.delete("folder_id")]),
  );
});
