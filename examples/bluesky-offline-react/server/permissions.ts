import { schema as s } from "jazz-tools";
import { app } from "../schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  // oauthSessions intentionally has no client policies. Only the backend can access it.
  policy.profiles.allowRead.where({});
  policy.posts.allowRead.where({});
  policy.postImages.allowRead.where({});
  policy.timelineEntries.allowRead.where({ ownerDid: session.user_id });
  policy.threadEntries.allowRead.where({});
  policy.likes.allowRead.where({});
  policy.reposts.allowRead.where({});

  policy.pendingOperations.allowRead.where({ ownerDid: session.user_id });
  policy.pendingOperations.allowInsert.where({ ownerDid: session.user_id });
  policy.pendingOperations.allowUpdate
    .whereOld({ ownerDid: session.user_id })
    .whereNew({ ownerDid: session.user_id });
  policy.pendingOperations.allowDelete.where({ ownerDid: session.user_id });

  policy.timelineEntries.allowInsert.where({ ownerDid: session.user_id });
  policy.timelineEntries.allowUpdate
    .whereOld({ ownerDid: session.user_id })
    .whereNew({ ownerDid: session.user_id });
  policy.timelineEntries.allowDelete.where({ ownerDid: session.user_id });
  policy.posts.allowInsert.where({ authorDid: session.user_id });
  policy.posts.allowUpdate
    .whereOld({ authorDid: session.user_id })
    .whereNew({ authorDid: session.user_id });
  policy.likes.allowInsert.where({ actorDid: session.user_id });
  policy.likes.allowUpdate
    .whereOld({ actorDid: session.user_id })
    .whereNew({ actorDid: session.user_id });
  policy.reposts.allowInsert.where({ actorDid: session.user_id });
  policy.reposts.allowUpdate
    .whereOld({ actorDid: session.user_id })
    .whereNew({ actorDid: session.user_id });
});
