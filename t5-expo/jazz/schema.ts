import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";
const schema = {
  notes: s.table({ text: s.string(), owner_id: s.uuid(), photo: s.bytes().optional() }, {}),
};
export const app = s.defineApp(schema, { migrations });
