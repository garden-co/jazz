import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.branches.allowRead.where({ owner_id: session.user_id });
  policy.branches.allowInsert.where({ owner_id: session.user_id });
  policy.branches.allowUpdate
    .whereOld({ owner_id: session.user_id })
    .whereNew({ owner_id: session.user_id });
  policy.branches.allowDelete.where({ owner_id: session.user_id });

  policy.articles.allowRead.where({});
  policy.articles.allowInsert.where({ owner_id: session.user_id });
  policy.articles.allowUpdate.where({});
  policy.articles.allowDelete.where({ owner_id: session.user_id });
});
