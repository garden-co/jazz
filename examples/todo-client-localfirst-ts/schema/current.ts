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
    owner_id: col.string(),
    parent: col.ref("todos").optional(),
    project: col.ref("projects").optional(),
  },
  {
    permissions: {
      select: policy.allow(),
      insert: policy.eqSession("owner_id", "user_id"),
      update: {
        using: policy.eqSession("owner_id", "user_id"),
        withCheck: policy.eqSession("owner_id", "user_id"),
      },
      delete: policy.eqSession("owner_id", "user_id"),
    },
  },
);
