import type { AppTransforms } from "./jazz.generated.js";

// Jazz passes null through without calling a transform.
export const transforms = {
  itemRef: { from: (value) => `item:${value}`, to: (value) => value.replace(/^item:/, "") },
  labelPrefix: { from: (value) => `label:${value}`, to: (value) => value.replace(/^label:/, "") },
  tenfold: { from: (value) => value * 10, to: (value) => value / 10 },
} satisfies AppTransforms;
