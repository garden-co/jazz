import type { AppComputed, AppTransforms } from "./jazz.generated.js";

export const computed = {
  things: {
    summary: (row) => `${row.text}:${row.int}`,
    maybeLabel: (row) => (row.maybeStatus === "high" ? "x" : "y"),
    score: (row) => row.float,
    shape: (row) => ({ w: row.float }),
  },
} satisfies AppComputed;

export const transforms = {
  upper: { from: (stored) => stored.toUpperCase(), to: (view) => view.toLowerCase() },
  iso: { from: (stored) => stored.toISOString(), to: (view) => new Date(view) },
} satisfies AppTransforms;
