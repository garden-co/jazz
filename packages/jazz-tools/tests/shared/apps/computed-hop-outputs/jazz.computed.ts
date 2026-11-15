import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  sources: { rank: () => 0, title: () => "source" },
  targets: { label: (row) => row.title.toUpperCase() },
} satisfies AppComputed;
