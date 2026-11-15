import type { AppComputed, AppTransforms } from "./jazz.generated.js";

export const computed = {
  rows: { summary: (row) => `${row.text}:${row.json.extra}:${row.bytes.length}` },
} satisfies AppComputed;

export const transforms = {
  upperCase: { from: (value) => value.toUpperCase(), to: (value) => value.toLowerCase() },
} satisfies AppTransforms;
