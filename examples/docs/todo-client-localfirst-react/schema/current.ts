import { table, col, policy } from "jazz-tools";

table("projects", {
  name: col.string(),
});

table(
  "todos",
  {
    title: col.string(),
    done: col.boolean(),
    description: col.string().optional(),
    parent: col.ref("todos").optional(),
    project: col.ref("projects").optional(),
  },
  {
    permissions: {
      select: policy.allow(),
      insert: policy.allow(),
      update: {
        using: policy.allow(),
        withCheck: policy.allow(),
      },
      delete: policy.allow(),
    },
  },
);
