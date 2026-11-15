// The computed fields and transforms jazz.app.json declares, implemented
// in TypeScript. Written by `jazz-tools upgrade` from schema.ts; edit freely.
import type { AppComputed, AppTransforms } from "./jazz.generated.js";
import { clamp } from "./src/clamp";

export type Priority = "low" | "medium" | "high";

const isoDate = {
  from: (stored: Date): string => stored.toISOString(),
  to: (view: string): Date => new Date(view),
};

export const computed = {
  tasks: {
    priority: (row): Priority => (row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low"),
    label: ({ title, score }) => `${title} (${score})`,
    done: (row) => row.score >= 10,
    doubled: (row) => row.score * 2,
    ratio: (row) => row.weight / 2,
    bounded: (row) => clamp(row.score),
    createdDay: (row) => new Date(row.id.length),
  },
} satisfies AppComputed;

export const transforms = {
  isoDate,
  tasksTags: {
    from: (stored) => stored.split(","),
    to: (view: string[]) => view.join(","),
  },
} satisfies AppTransforms;
