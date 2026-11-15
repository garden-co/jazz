import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: {
    parsed: (row) => {
      try {
        return JSON.parse(row.text);
      } catch (cause) {
        throw new Error("Invalid JSON", { cause });
      }
    },
  },
} satisfies AppComputed;
