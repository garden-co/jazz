import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";

// Enum columns with cases validate on alpha.63/.64 but writes fail there
// (jazz#5621), and alpha.65 removes them (jazz#5758): upgrade refuses.
const schema = {
  issues: s.table(
    {
      title: s.string(),
      kind: s.enum({ bug: { severity: s.int() }, feature: { area: s.string() } }),
    },
    {},
  ),
};

export const app: s.App<s.Schema<typeof schema>> = s.defineApp(schema, { migrations });
