import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app";

export default definePermissions(app, ({ policy, either, both, allowedTo, session }) => [
  // #region permissions-simple-ts
  policy.todos.allowRead.where({ owner_id: session.user_id }),
  policy.todos.allowInsert.where({ owner_id: session.user_id }),
  policy.todos.allowUpdate
    .whereOld(both({ owner_id: session.user_id }).and({ done: false }))
    .whereNew({ owner_id: session.user_id }),
  policy.todos.allowDelete.where({ owner_id: session.user_id }),
  // #endregion permissions-simple-ts

  // #region permissions-allowed-to-ts
  policy.todos.allowRead.where(either({ done: false }).or(allowedTo.read("project"))),
  policy.todos.allowUpdate
    .whereOld(both(allowedTo.update("project")).and({ done: false }))
    .whereNew(allowedTo.update("project")),
  // #endregion permissions-allowed-to-ts
]);
