// #region schema-svelte
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

// #endregion schema-svelte

const { definePermissions } = schema;

export const permissions = definePermissions({
  projects: {
    read: true,
    insert: true,
  },
  todos: {
    read: true,
    insert: { done: false },
    updateIf: { done: false },
    updateTo: true,
    delete: { done: false },
  },
});
