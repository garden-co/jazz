import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  projects: s.table(
    { name: s.string() },
    {
      todos: s.reverse("todos", "project"),
    },
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
      children: s.reverse("todos", "parent"),
      parent: s.rel("todos", "parentId"),
      project: s.rel("projects", "projectId"),
    },
  ),
});

const { definePermissions, session } = schema;

const ownedByMe = { owner_id: session.user };

export const permissions = definePermissions({
  projects: {
    read: true,
  },
  todos: {
    read: ownedByMe,
    insert: ownedByMe,
    update: ownedByMe,
    delete: ownedByMe,
  },
});
