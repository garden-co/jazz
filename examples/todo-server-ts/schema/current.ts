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
    owner_id: col.string(),
  },
  {
    permissions: {
      select: policy.eqSession("owner_id", "user_id"),
      insert: policy.eqSession("owner_id", "user_id"),
      update: {
        using: policy.eqSession("owner_id", "user_id"),
        withCheck: policy.eqSession("owner_id", "user_id"),
      },
      delete: policy.eqSession("owner_id", "user_id"),
    },
  },
);
