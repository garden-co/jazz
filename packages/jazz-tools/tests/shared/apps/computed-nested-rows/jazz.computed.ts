import type { AppComputed, Row } from "./jazz.generated.js";

// Results holding the callback's row itself (not JSON values): the read
// completes the row references before returning them.
export const computed = {
  rows: {
    wrapper: (row: Row) => Object.freeze({ nested: [{ saved: row }] }),
    mapping: (row: Row) => new Map([[row, row]]),
    membership: (row: Row) => new Set([row]),
  },
} as unknown as AppComputed;
