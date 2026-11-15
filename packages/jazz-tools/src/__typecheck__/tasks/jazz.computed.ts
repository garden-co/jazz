import type { AppComputed, AppTransforms } from "./jazz.generated.js";

export const computed = {
  tasks: {
    priority: (row) => (row.score >= 8 || row.status === "doing" ? "high" : "low"),
    label: (row) => (row.dueAt ? `${row.title} (${row.dueAt})` : null),
  },
} satisfies AppComputed;

export const transforms = {
  isoDate: { from: (stored) => stored.toISOString(), to: (view) => new Date(view) },
  lowercase: { from: (stored) => stored, to: (view) => view.toLowerCase() },
} satisfies AppTransforms;
