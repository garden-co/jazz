import type { AppComputed, Row } from "./jazz.generated.js";

// The result holds the callback's row itself, not a JSON value.
export const computed = {
  rows: { wrapper: (row: Row) => ({ saved: row }) },
} as unknown as AppComputed;
