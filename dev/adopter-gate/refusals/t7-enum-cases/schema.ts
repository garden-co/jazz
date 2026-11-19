// Enum with cases (payload enum). On .64 it validates, but writes fail (jazz#5621).
// .65 removes payload enums: `jazz-tools upgrade` should refuse this app with a clear error naming tickets.kind.
import { schema as s } from "jazz-tools";
import { migrations } from "./migrations/index.js";
const schema = {
  tickets: s.table({ title: s.string(), owner_id: s.uuid(), kind: s.enum({ bug: { severity: s.int() }, feature: { area: s.string() } }).optional() }, {}),
};
export const app = s.defineApp(schema, { migrations });
