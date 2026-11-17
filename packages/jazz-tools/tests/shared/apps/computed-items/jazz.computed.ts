import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  items: {
    priority: (row) => (row.score >= 8 ? "high" : "low"),
    description: (row) => `${row.id}:${row.text.length}`,
  },
} satisfies AppComputed;
