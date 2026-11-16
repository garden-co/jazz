import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  sources: { rank: () => 0 },
  targets: { doubled: (row) => row.rank * 2 },
} satisfies AppComputed;
