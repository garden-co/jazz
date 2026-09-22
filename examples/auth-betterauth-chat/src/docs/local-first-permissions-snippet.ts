import { definePermissions } from "jazz-tools/permissions";
import { app } from "../../schema";

// #region local-first-permissions
export default definePermissions(app, ({ policy, allOf, session }) => {
  const isLocalFirstAuthMode = session.where({
    authMode: "local-first",
  });

  // Everyone can read
  policy.messages.allowRead.always();

  // Local-first users can only read — block inserts until they sign up
  policy.messages.allowInsert.where(allOf([{ not: isLocalFirstAuthMode }]));
});
// #endregion local-first-permissions
