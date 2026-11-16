import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";

// An earlier version had a `legacy_kinds` table with an enum with cases
// (removed in alpha.65, jazz#5758); a migration dropped it. That version stays
// in the history, which a store that admitted it still holds.
const schema = {
  tickets: s.table(
    {
      title: s.string(),
      category: s.enum("bug", "feature").optional(),
      severity: s.int().optional(),
      area: s.string().optional(),
    },
    {},
  ),
};

export const app: s.App<s.Schema<typeof schema>> = s.defineApp(schema, { migrations });
