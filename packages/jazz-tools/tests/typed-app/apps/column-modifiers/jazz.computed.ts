import type { AppTransforms } from "./jazz.generated.js";

export const transforms = {
  length: { from: (stored) => stored.length, to: (view) => view.toString() },
} satisfies AppTransforms;
