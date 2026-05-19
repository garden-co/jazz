import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, anyOf, allowedTo, session }) => {
  // Articles: anyone can read published; authors see their own (even unpublished).
  policy.articles.allowRead.where(anyOf([{ published: true }, { authorId: session.user_id }]));
  policy.articles.allowInsert.where({ authorId: session.user_id });
  policy.articles.allowUpdate
    .whereOld({ authorId: session.user_id })
    .whereNew({ authorId: session.user_id });
  policy.articles.allowDelete.where({ authorId: session.user_id });

  // Drafts: visible only to their owner.
  policy.drafts.allowRead.where({ ownerId: session.user_id });
  policy.drafts.allowInsert.where({ ownerId: session.user_id });
  policy.drafts.allowUpdate
    .whereOld({ ownerId: session.user_id })
    .whereNew({ ownerId: session.user_id });
  policy.drafts.allowDelete.where({ ownerId: session.user_id });

  // A draft row can act as a branch anchor for the article it points at.
  // Because drafts.allowRead requires ownerId == session.user_id, only the
  // draft's owner can resolve $branch — so only they can read/write the
  // branch overlay.
  policy.articles.forBranch(policy.drafts, ({ $branch, branchPolicy }) => {
    branchPolicy.allowRead.where({ id: $branch.articleId });
    branchPolicy.allowInsert.where({ id: $branch.articleId });
    branchPolicy.allowUpdate
      .whereOld({ id: $branch.articleId })
      .whereNew({ id: $branch.articleId });
    branchPolicy.allowDelete.where({ id: $branch.articleId });
  });

  // Cover images. The CMS is public: anyone reading a published article can
  // fetch its cover, so files and file_parts are world-readable. Inserts are
  // open to any authed session (you must be signed in to upload). Deletes are
  // gated through an `image_uploads` row owned by the uploader.
  policy.files.allowRead.where({});
  policy.files.allowInsert.where({});
  policy.file_parts.allowRead.where({});
  policy.file_parts.allowInsert.where({});

  policy.image_uploads.allowRead.where({ ownerId: session.user_id });
  policy.image_uploads.allowInsert.where({ ownerId: session.user_id });
  policy.image_uploads.allowDelete.where({ ownerId: session.user_id });

  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.image_uploads, "fileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
