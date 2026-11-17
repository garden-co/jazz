import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";
import { clamp } from "./src/clamp";

export type Priority = "low" | "medium" | "high";

const isoDate = {
  from: (stored: Date): string => stored.toISOString(),
  to: (view: string): Date => new Date(view),
};

const schema = {
  tasks: s.table(
    {
      title: s.string(),
      score: s.int(),
      weight: s.float(),
      dueAt: s.timestamp().transform(isoDate),
      tags: s.string().transform({
        from: (stored) => stored.split(","),
        to: (view: string[]) => view.join(","),
      }),
    },
    {
      priority: s.computed(
        (row): Priority => (row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low"),
      ),
      label: s.computed(({ title, score }) => `${title} (${score})`),
      done: s.computed((row) => row.score >= 10),
      doubled: s.computed((row) => row.score * 2),
      ratio: s.computed((row) => row.weight / 2),
      bounded: s.computed((row) => clamp(row.score)),
      createdDay: s.computed((row) => new Date(row.id.length)),
    },
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema, { migrations });

export type Task = s.RowOf<typeof app.tasks>;
