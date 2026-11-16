import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: {
    toString: () => "custom",
    valueOf: () => "custom value",
    tag: (row) => row.toString(),
    original: (row) => row.valueOf() === row,
  },
} satisfies AppComputed;
