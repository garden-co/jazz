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
      parentId: s.uuid().optional(),
      projectId: s.uuid().optional(),
      owner_id: s.uuid(),
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
    read: ownerIdIsMe,
    insert: ownerIdIsMe,
    update: ownerIdIsMe,
    delete: ownerIdIsMe,
  },
});
