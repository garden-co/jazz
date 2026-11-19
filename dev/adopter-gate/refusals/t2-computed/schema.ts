import { schema as s } from "jazz-tools";
import { migrations } from "./migrations/index.js";
type Priority = "low" | "medium" | "high";
const tasks = s.table(
  {
    title: s.string(),
    score: s.int(),
    estimate: s.float().optional(),
    owner_id: s.uuid(),
    // transform: stored as timestamp, app sees ISO string
    dueAt: s.timestamp().optional().transform({
      from: (v) => (v == null ? null : new Date(v as any).toISOString().slice(0, 10)),
      to: (v: string | null) => (v == null ? null : (new Date(v + "T00:00:00Z") as any)),
    }),
    tags: s.array(s.string()),
    meta: s.json().optional(),
  },
  {
    priority: s.computed((row): Priority => (row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low")),
    label: s.computed((row) => `${row.title} (${row.score})`),
  },
);
export const app = s.defineApp({ tasks }, { migrations });
