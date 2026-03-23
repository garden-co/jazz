import { schema as s } from "jazz-tools";
import { app } from "../schema.js";

// #region permissions-schema-ts
const schema = {
  projects: s.table({
    name: s.string(),
    owner_id: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    parentId: s.ref("todos").optional(),
    projectId: s.ref("projects").optional(),
    owner_id: s.string(),
  }),
  todoShares: s.table({
    todoId: s.ref("todos"),
    user_id: s.string(),
    can_read: s.boolean(),
  }),
};
// #endregion permissions-schema-ts

// #region permissions-simple-ts
s.definePermissions(app, ({ policy, allOf, session }) => {
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
s.definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.always();
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.always();
  policy.todos.allowDelete.always();
});
// #endregion permissions-always-ts

// #region permissions-never-ts
s.definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.never();
  policy.todos.allowInsert.never();
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});
// #endregion permissions-never-ts

// #region permissions-allowed-to-ts
s.definePermissions(app, ({ policy, anyOf, allOf, allowedTo }) => {
  // Users can read a todo if it's not done, or if they can read its project.
  policy.todos.allowRead.where(anyOf([{ done: false }, allowedTo.read("project")]));
  // Users can update a todo if they can update its project and it's not done.
  policy.todos.allowUpdate
    .whereOld(allOf([allowedTo.update("project"), { done: false }]))
    .whereNew(allowedTo.update("project"));
});
// #endregion permissions-allowed-to-ts

// #region permissions-combinators-ts
s.definePermissions(app, ({ policy, allOf, anyOf, allowedTo, session }) => {
  // Users can read a todo if they own it, or if it's not done and they can read its project.
  policy.todos.allowRead.where(
    anyOf([{ owner_id: session.user_id }, allOf([{ done: false }, allowedTo.read("project")])]),
  );
});
// #endregion permissions-combinators-ts

// #region permissions-session-claims-ts
s.definePermissions(app, ({ policy, anyOf, session }) => {
  policy.todos.allowRead.where(
    anyOf([{ owner_id: session.user_id }, session.where({ "claims.role": "manager" })]),
  );
});
// #endregion permissions-session-claims-ts

// #region permissions-recursive-inherits-ts
s.definePermissions(app, ({ policy, allowedTo }) => {
  // Users can read a todo if they can read its parent (follows the chain upward).
  policy.todos.allowRead.where(allowedTo.read("parent"));
  // Users can update a todo if they can update its parent, up to 5 levels deep.
  policy.todos.allowUpdate
    .whereOld(allowedTo.update("parent", { maxDepth: 5 }))
    .whereNew(allowedTo.update("parent", { maxDepth: 5 }));
});
// #endregion permissions-recursive-inherits-ts

// #region permissions-shares-ts
s.definePermissions(app, ({ policy, anyOf, session }) => {
  // Users can read a todo if they own it, or if someone shared it with them.
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
