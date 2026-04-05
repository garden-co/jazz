import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

// #region permissions-basic-expo
export default s.definePermissions(app, ({ policy, allOf, session }) => [
  // Each user only sees their own rows.
  policy.todos.allowRead.where({ owner_id: session.user_id }),
  // New rows must belong to the current user.
  policy.todos.allowInsert.where({ owner_id: session.user_id }),
  // Users can only mutate their own incomplete todos.
  policy.todos.allowUpdate
    .whereOld(allOf([{ owner_id: session.user_id }, { done: false }]))
    .whereNew({ owner_id: session.user_id }),
  policy.todos.allowDelete.where(allOf([{ owner_id: session.user_id }, { done: false }])),
]);
// #endregion permissions-basic-expo
