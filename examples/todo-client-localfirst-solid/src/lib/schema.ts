import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  projects: s.table(
    {
      name: s.string(),
    },
    { todosViaProject: s.reverse("todos", "project") },
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
      todosViaParent: s.reverse("todos", "parent"),
      project: s.rel("projects", "projectId"),
    },
  ),
});

const { definePermissions, session } = schema;

const ownerIdIsMe = { owner_id: session.user };

export const permissions = definePermissions({
  projects: {
    read: true,
    insert: true,
  },
  todos: {
    read: true,
    insert: ownerIdIsMe,
    update: ownerIdIsMe,
    delete: ownerIdIsMe,
  },
});
