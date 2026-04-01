import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

// #region permissions-basic-expo
export default s.definePermissions(app, ({ policy, allOf, session }) => [
  // Each user only sees their own rows.
  policy.todos.allowRead.where({ ownerId: session.user_id }),
  // New rows must belong to the current user.
  policy.todos.allowInsert.where({ ownerId: session.user_id }),
  // Users can only mutate their own incomplete todos.
  policy.todos.allowUpdate
    .whereOld(allOf([{ ownerId: session.user_id }, { done: false }]))
    .whereNew({ ownerId: session.user_id }),
  policy.todos.allowDelete.where(allOf([{ ownerId: session.user_id }, { done: false }])),
]);
// #endregion permissions-basic-expo
