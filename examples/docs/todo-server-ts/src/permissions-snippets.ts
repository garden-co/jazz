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
definePermissions(app, ({ policy, allOf, session }) => {
  policy.todos.allowRead.where({ owner_id: session.user_id });
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate
    .whereOld(allOf([{ owner_id: session.user_id }, { done: false }]))
    .whereNew({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });
});
// #endregion permissions-simple-ts

// #region permissions-always-ts
definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.always();
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.always();
  policy.todos.allowDelete.always();
});
// #endregion permissions-always-ts

// #region permissions-never-ts
definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.never();
  policy.todos.allowInsert.never();
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});
// #endregion permissions-never-ts

// #region permissions-allowed-to-ts
definePermissions(app, ({ policy, anyOf, allOf, allowedTo }) => {
  policy.todos.allowRead.where(anyOf([{ done: false }, allowedTo.read("project")]));
  policy.todos.allowUpdate
    .whereOld(allOf([allowedTo.update("project"), { done: false }]))
    .whereNew(allowedTo.update("project"));
});
// #endregion permissions-allowed-to-ts

// #region permissions-combinators-ts
definePermissions(app, ({ policy, allOf, anyOf, allowedTo, session }) => {
  policy.todos.allowRead.where(
    anyOf([{ owner_id: session.user_id }, allOf([{ done: false }, allowedTo.read("project")])]),
  );
});
// #endregion permissions-combinators-ts

// #region permissions-recursive-inherits-ts
definePermissions(app, ({ policy, allowedTo }) => {
  policy.todos.allowRead.where(allowedTo.read("parent"));
  policy.todos.allowUpdate
    .whereOld(allowedTo.update("parent", { maxDepth: 5 }))
    .whereNew(allowedTo.update("parent", { maxDepth: 5 }));
});
// #endregion permissions-recursive-inherits-ts
