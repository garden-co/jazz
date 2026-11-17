import { expectTypeOf } from "vitest";
import type { AppComputed, AppTransforms, ItemPriority } from "./jazz.generated.js";

export const computed = {
  items: {
    priority: (row) => {
      expectTypeOf(row.score).toEqualTypeOf<number>();
      expectTypeOf(row.storedPriority).toEqualTypeOf<ItemPriority>();
      expectTypeOf(row.id).toEqualTypeOf<string>();
      return row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low";
    },
  },
} satisfies AppComputed;

export const transforms = {
  priority: {
    from: (score) => (score >= 8 ? "high" : score >= 4 ? "medium" : "low"),
    to: (priority) => ({ low: 1, medium: 5, high: 10 })[priority],
  },
} satisfies AppTransforms;
