import { schema as s } from "jazz-tools";

// Example: dropping a column with a backwards default.
// Clients still on the older schema continue seeing legacy_priority.
export default s.defineMigration({
  migrate: {
    todos: {
      legacy_priority: s.drop.int({ backwardsDefault: 0 }),
    },
  },
  fromHash: "311995e9a178",
  toHash: "73b65d082ab8",
  from: {
    todos: s.table(
      {
        title: s.string(),
        done: s.boolean(),
        description: s.string().optional(),
        parentId: s.uuid().optional(),
        projectId: s.uuid().optional(),
        owner_id: s.string(),
        legacy_priority: s.int(),
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
