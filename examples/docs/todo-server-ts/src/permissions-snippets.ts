import { col, table } from "jazz-tools";
import { definePermissions } from "jazz-tools/permissions";
import { app } from "../schema/app.js";

// #region permissions-schema-ts
table("projects", {
  name: col.string(),
  owner_id: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  project: col.ref("projects").optional(),
  owner_id: col.string(),
});

table("todoShares", {
  todo: col.ref("todos"),
  user_id: col.string(),
  can_read: col.boolean(),
});
// #endregion permissions-schema-ts

// #region permissions-simple-ts
definePermissions(app, ({ policy, both, session }) => [
  policy.todos.allowRead.where({ owner_id: session.user_id }),
  policy.todos.allowInsert.where({ owner_id: session.user_id }),
  policy.todos.allowUpdate
    .whereOld(both({ owner_id: session.user_id }).and({ done: false }))
    .whereNew({ owner_id: session.user_id }),
  policy.todos.allowDelete.where({ owner_id: session.user_id }),
]);
// #endregion permissions-simple-ts

// #region permissions-allowed-to-ts
definePermissions(app, ({ policy, either, both, allowedTo }) => [
  policy.todos.allowRead.where(either({ done: false }).or(allowedTo.read("project"))),
  policy.todos.allowUpdate
    .whereOld(both(allowedTo.update("project")).and({ done: false }))
    .whereNew(allowedTo.update("project")),
]);
// #endregion permissions-allowed-to-ts
