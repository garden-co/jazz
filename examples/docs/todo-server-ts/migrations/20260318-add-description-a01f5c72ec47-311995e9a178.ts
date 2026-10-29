import { schema as s, migration as m } from "jazz-tools";

// Example of editing a generated migration stub.
export default m.defineMigration({
  migrate: {
    todos: {
      description: m.add.string({ default: "No description" }),
    },
  },
  fromHash: "a01f5c72ec47",
  toHash: "311995e9a178",
  from: {
    todos: s.table(
      {
        title: s.string(),
        done: s.boolean(),
        parentId: s.uuid().optional(),
        projectId: s.uuid().optional(),
        owner_id: s.string(),
      },
      {
        parent: s.rel("todos", "parentId"),
        children: s.reverse("todos", "parent"),
        project: s.rel("projects", "projectId"),
      },
    ),
  },
  to: {
    todos: s.table(
      {
        title: s.string(),
        done: s.boolean(),
        description: s.string().optional(),
        parentId: s.uuid().optional(),
        projectId: s.uuid().optional(),
        owner_id: s.string(),
      },
      {
        parent: s.rel("todos", "parentId"),
        children: s.reverse("todos", "parent"),
        project: s.rel("projects", "projectId"),
      },
    ),
  },
});
