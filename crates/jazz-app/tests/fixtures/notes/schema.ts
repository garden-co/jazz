import { schema as s } from "jazz-tools";
export const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) }, { migrations: "none" });
