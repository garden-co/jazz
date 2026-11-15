import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: { constant: () => 42, identity: (row) => row.id },
} satisfies AppComputed;
