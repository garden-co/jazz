import type { AppComputed } from "./jazz.generated.js";

// Asynchronous callbacks are unsupported: the read reports them, and their
// rejections stay contained. The casts stand in for the DSL's untyped callbacks.
export const computed = {
  rows: {
    missing: (async (row: { score: number }) => row.score) as unknown as (row: {
      score: number;
    }) => number,
    failing: (async () => {
      throw new Error("async callback failure");
    }) as unknown as () => number,
  },
} satisfies AppComputed;
