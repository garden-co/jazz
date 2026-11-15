import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";

const schema = {
  items: s.table(
    { score: s.int() },
    {
      doubled: s.computed((row) => row.score * 2),
      quadrupled: s.computed((row) => row.doubled * 2),
    },
  ),
};

export const app: s.App<s.Schema<typeof schema>> = s.defineApp(schema, { migrations });
