import { schema as s, table } from "jazz-tools";

table("discarded_side_effect", {
  title: s.string(),
});

export const schema = {
  explicit_tasks: s.table(
    {
      title: s.string(),
    },
    {},
  ),
};
