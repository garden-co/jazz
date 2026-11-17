import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: {
    parsed: (row) => {
      try {
        return JSON.parse(row.text);
      } catch {
        return row.text;
      }
    },
  },
} satisfies AppComputed;
