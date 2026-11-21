// #region schema-react
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

// #endregion schema-react

// #region permissions-basic-react
const { definePermissions } = schema;

export const permissions = definePermissions({
  projects: {
    read: true,
    insert: true,
  },
  todos: {
    // Everyone can read todos.
    read: true,
    // New todos start as incomplete.
    insert: { done: false },
    // Completed todos are immutable.
    updateIf: { done: false },
    updateTo: true,
    // Only open todos can be deleted.
    delete: { done: false },
  },
});
// #endregion permissions-basic-react
