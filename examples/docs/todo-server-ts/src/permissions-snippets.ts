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
  parentId: col.ref("todos").optional(),
  projectId: col.ref("projects").optional(),
  owner_id: col.string(),
});

table("todoShares", {
  todoId: col.ref("todos"),
  user_id: col.string(),
  can_read: col.boolean(),
});
// #endregion permissions-schema-ts

// #region permissions-simple-ts
definePermissions(app, ({ policy, allOf, session }) => {
  // Users can only read their own todos
  policy.todos.allowRead.where({ owner_id: session.user_id });
  // Users cannot create todos with different owners
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  // Users can update their own todos, but only if not already done
  policy.todos.allowUpdate
    .whereOld(allOf([{ owner_id: session.user_id }, { done: false }]))
    .whereNew({ owner_id: session.user_id });
  // Users can only delete their own todos
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
  // Users can read a todo if it's not done, or if they can read its project
  policy.todos.allowRead.where(anyOf([{ done: false }, allowedTo.read("project")]));
  // Users can update a todo if they can update its project and it's not done
  policy.todos.allowUpdate
    .whereOld(allOf([allowedTo.update("project"), { done: false }]))
    .whereNew(allowedTo.update("project"));
});
// #endregion permissions-allowed-to-ts

// #region permissions-combinators-ts
definePermissions(app, ({ policy, allOf, anyOf, allowedTo, session }) => {
  // Users can read a todo if they own it, or if it's not done and they can read its project
  policy.todos.allowRead.where(
    anyOf([{ owner_id: session.user_id }, allOf([{ done: false }, allowedTo.read("project")])]),
  );
});
// #endregion permissions-combinators-ts

// #region permissions-recursive-inherits-ts
definePermissions(app, ({ policy, allowedTo }) => {
  // Users can read a todo if they can read its parent (follows the chain upward)
  policy.todos.allowRead.where(allowedTo.read("parent"));
  // Users can update a todo if they can update its parent, up to 5 levels deep
  policy.todos.allowUpdate
    .whereOld(allowedTo.update("parent", { maxDepth: 5 }))
    .whereNew(allowedTo.update("parent", { maxDepth: 5 }));
});
// #endregion permissions-recursive-inherits-ts

// #region permissions-shares-ts
definePermissions(app, ({ policy, anyOf, session }) => {
  // Users can read a todo if they own it, or if someone shared it with them
  policy.todos.allowRead.where((todo) =>
    anyOf([
      { owner_id: session.user_id },
      policy.todoShares.exists.where({
        todoId: todo.id,
        user_id: session.user_id,
        can_read: true,
      }),
    ]),
  );
});
// #endregion permissions-shares-ts

// #region permissions-session-claims-ts
definePermissions(app, ({ policy, anyOf, session }) => {
  policy.todos.allowRead.where(
    anyOf([{ owner_id: session.user_id }, session.where({ "claims.role": "manager" })]),
  );
});
// #endregion permissions-session-claims-ts
