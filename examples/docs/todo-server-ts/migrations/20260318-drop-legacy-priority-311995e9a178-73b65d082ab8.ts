import { col, defineMigration } from "jazz-tools";

// Example: dropping a column with a backwards default.
// Clients still on the older schema continue seeing legacy_priority.
export default defineMigration({
  migrate: {
    todos: {
      legacy_priority: col.drop.int({ backwardsDefault: 0 }),
    },
  },
  fromHash: "311995e9a178",
  toHash: "73b65d082ab8",
  from: {
    todos: {
      title: col.string(),
      done: col.boolean(),
      description: col.string().optional(),
      parentId: col.ref("todos").optional(),
      projectId: col.ref("projects").optional(),
      owner_id: col.string(),
      legacy_priority: col.int(),
    },
  },
  to: {
    todos: {
      title: col.string(),
      done: col.boolean(),
      description: col.string().optional(),
      parentId: col.ref("todos").optional(),
      projectId: col.ref("projects").optional(),
      owner_id: col.string(),
    },
  },
});
