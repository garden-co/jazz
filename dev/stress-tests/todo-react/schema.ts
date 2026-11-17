import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  projects: s.table(
    { name: s.string() },
    {
      todosViaProject: s.reverse("todos", "project"),
    },
  ),
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      description: s.string().optional(),
      owner_id: s.string(),
      parentId: s.uuid().optional(),
      projectId: s.uuid().optional(),
    },
    {
      parent: s.rel("todos", "parentId"),
      project: s.rel("projects", "projectId"),
      todosViaParent: s.reverse("todos", "parent"),
    },
  ),
});

const { definePermissions } = schema;

export const permissions = definePermissions({
  projects: {
    read: true,
    insert: true,
    update: true,
    delete: true,
  },
  todos: {
    read: true,
    insert: true,
    update: true,
    delete: true,
  },
});
