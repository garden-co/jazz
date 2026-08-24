import { schema as s } from "jazz-tools";
import { app } from "./schema";

// The first implementation deliberately keeps permission scope visible in the
// schema: membership is workspace-scoped, while pages/blocks inherit through
// their workspace/page relation. The exact recursive policy lowering is a
// required app-facing E2E concern, not a client-side filtering convention.
export default s.definePermissions(app, ({ policy, session }) => {
  policy.workspaces.allowRead.where({ $createdBy: session.user_id });
  policy.workspaces.allowInsert.always();
  policy.workspaces.allowUpdate.where({ $createdBy: session.user_id });

  policy.members.allowRead.where({ subject: session.user_id });
  policy.members.allowInsert.always();

  // Product policy is intentionally conservative until the recursive
  // workspace-membership traversal is wired and tested with real sessions.
  policy.pages.allowRead.where({ $createdBy: session.user_id });
  policy.pages.allowInsert.always();
  policy.pages.allowUpdate.where({ $createdBy: session.user_id });
  policy.blocks.allowRead.where({ $createdBy: session.user_id });
  policy.blocks.allowInsert.always();
  policy.blocks.allowUpdate.where({ $createdBy: session.user_id });
  policy.suggestions.allowRead.where({ $createdBy: session.user_id });
  policy.suggestions.allowInsert.always();
  policy.attachments.allowRead.where({ $createdBy: session.user_id });
  policy.attachments.allowInsert.always();
});
