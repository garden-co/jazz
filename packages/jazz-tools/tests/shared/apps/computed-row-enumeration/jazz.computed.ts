import type { AppComputed, Row } from "./jazz.generated.js";

// `copy` returns the callback's row itself, not a JSON value.
export const computed = {
  rows: {
    json: (row: Row) => JSON.stringify(row),
    copy: (row: Row) => row,
    keys: (row: Row) => Object.keys(row).sort(),
    tag: (row: Row) => row.toString(),
  },
} as unknown as AppComputed;
