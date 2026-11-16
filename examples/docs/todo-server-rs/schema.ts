import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  projects: s.table(
    {
      name: s.string(),
    },
    { todos: s.reverse("todos", "projectRelation") },
  ),
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      priority: s.int().optional(),
      description: s.string().optional(),
      parent: s.uuid().optional(),
      project: s.uuid().optional(),
    },
    {
      parentRelation: s.rel("todos", "parent"),
      children: s.reverse("todos", "parentRelation"),
      projectRelation: s.rel("projects", "project"),
    },
  ),
});

const { definePermissions } = schema;

export const permissions = definePermissions({
  projects: {
    read: true,
    insert: true,
  },
  todos: {
    read: true,
    insert: true,
    update: true,
    delete: true,
  },
});
