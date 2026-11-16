import type { AppTransforms } from "./jazz.generated.js";

export const transforms = {
  priority: {
    from: (score) => (score >= 8 ? "high" : score >= 4 ? "medium" : "low"),
    to: (priority) => ({ low: 1, medium: 5, high: 10 })[priority],
  },
} satisfies AppTransforms;
