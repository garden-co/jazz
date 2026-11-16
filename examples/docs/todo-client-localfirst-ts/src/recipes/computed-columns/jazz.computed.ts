// The computed fields and transforms jazz.app.json declares, implemented
// in TypeScript. Written by `jazz-tools upgrade` from schema.ts; edit freely.
import type { AppComputed } from "./jazz.generated.js";

type Priority = "low" | "medium" | "high";

export const computed = {
  tasks: {
    priority: (row): Priority => (row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low"),
  },
} satisfies AppComputed;
