import { schema as s } from "jazz-tools";
import { app } from "../schema.js";

// #region permissions-schema-ts
const schema = {
  projects: s.table({
    name: s.string(),
    ownerId: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    projectId: s.ref("projects").optional(),
    owner_id: s.string(),
  }),
  todoShares: s.table({
    todoId: s.ref("todos"),
    userId: s.string(),
    can_read: s.boolean(),
  }),
};
// #endregion permissions-schema-ts

// #region permissions-simple-ts
s.definePermissions(app, ({ policy, allOf, session }) => {
  policy.todos.allowRead.where({ owner_id: session.user_id });
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate
    .whereOld(allOf([{ owner_id: session.user_id }, { done: false }]))
    .whereNew({ owner_id: session.user_id });
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
  policy.todos.allowRead.where(anyOf([{ done: false }, allowedTo.read("project")]));
  policy.todos.allowUpdate
    .whereOld(allOf([allowedTo.update("project"), { done: false }]))
    .whereNew(allowedTo.update("project"));
});
// #endregion permissions-allowed-to-ts

// #region permissions-combinators-ts
s.definePermissions(app, ({ policy, allOf, anyOf, allowedTo, session }) => {
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
  policy.todos.allowRead.where(allowedTo.read("parent"));
  policy.todos.allowUpdate
    .whereOld(allowedTo.update("parent", { maxDepth: 5 }))
    .whereNew(allowedTo.update("parent", { maxDepth: 5 }));
});
// #endregion permissions-recursive-inherits-ts
