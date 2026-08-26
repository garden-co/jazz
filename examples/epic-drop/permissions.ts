import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

// Foundation-only private folders. Shared-folder membership and revocation need
// their own application schema and product contract.
export default s.definePermissions(app, ({ policy, session }) => {
  policy.folders.allowRead.where({ owner_id: session.user_id });
  policy.folders.allowInsert.where({ owner_id: session.user_id });
  policy.folders.allowUpdate
    .whereOld({ owner_id: session.user_id })
    .whereNew({ owner_id: session.user_id });
  policy.folders.allowDelete.where({ owner_id: session.user_id });

  policy.files.allowRead.where({ owner_id: session.user_id });
  policy.files.allowInsert.where({ owner_id: session.user_id });
  policy.files.allowUpdate
    .whereOld({ owner_id: session.user_id })
    .whereNew({ owner_id: session.user_id });
  policy.files.allowDelete.where({ owner_id: session.user_id });
});
