import { schema as s } from "jazz-tools";

export const schema = {
  categories: s.table({ label: s.string() }),
  records: s.table({ category_ids: s.array(s.ref("categories")) }),
};
