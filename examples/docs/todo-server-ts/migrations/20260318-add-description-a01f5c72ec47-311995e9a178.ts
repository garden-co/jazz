import { col, defineMigration } from "jazz-tools";

// Example of editing a generated migration stub.
export default defineMigration({
  migrate: {
    todos: {
      description: col.add.string({ default: "No description" }),
    },
  },
  fromHash: "a01f5c72ec47",
  toHash: "311995e9a178",
  from: {
    todos: {
      title: col.string(),
      done: col.boolean(),
      parentId: col.ref("todos").optional(),
      projectId: col.ref("projects").optional(),
      owner_id: col.string(),
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
