import type { AppTransforms } from "./jazz.generated.js";

export const transforms = {
  urgency: {
    from: (stored) => (stored > 5 ? "high" : "low"),
    to: (view) => (view === "high" ? 10 : 1),
  },
} satisfies AppTransforms;
