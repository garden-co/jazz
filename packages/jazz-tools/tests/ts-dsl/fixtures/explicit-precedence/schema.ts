import { col, schema as s, table } from "jazz-tools";

table("discarded_side_effect", {
  title: col.string(),
});

export const schema = {
  explicit_tasks: s.table({
    title: s.string(),
  }),
};
