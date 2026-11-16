// #region schema-expo
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  projects: s.table(
    {
      name: s.string(),
    },
    { todos: s.reverse("todos", "project") },
  ),
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      description: s.string().optional(),
      owner_id: s.uuid(),
      parentId: s.uuid().optional(),
      projectId: s.uuid().optional(),
    },
    {
      parent: s.rel("todos", "parentId"),
      children: s.reverse("todos", "parent"),
      project: s.rel("projects", "projectId"),
    },
  ),
});

// #endregion schema-expo

// #region permissions-basic-expo
const { definePermissions, session } = schema;

const ownedByMe = { owner_id: session.user };

export const permissions = definePermissions({
  projects: {
    read: true,
    insert: true,
  },
  todos: {
    // Each user only sees their own rows.
    read: ownedByMe,
    // New rows must belong to the current user.
    insert: ownedByMe,
    // Users can only mutate their own incomplete todos.
    updateIf: { ...ownedByMe, done: false },
    updateTo: ownedByMe,
    delete: { ...ownedByMe, done: false },
  },
});
// #endregion permissions-basic-expo
