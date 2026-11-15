import type { JsonValue } from "jazz-tools";
import type { AppComputed, AppTransforms } from "./jazz.generated.js";

/** Reading `data` runs its transform, whose JSON.parse error the field catches. */
const readFailure = (row: { readonly data: JsonValue }): JsonValue => {
  try {
    return row.data;
  } catch (error) {
    return error instanceof SyntaxError ? "invalid JSON" : String(error);
  }
};

export const computed = {
  rows: {
    first: readFailure,
    second: readFailure,
    repeated: (row) => [readFailure(row), readFailure(row)],
  },
} satisfies AppComputed;

export const transforms = {
  jsonText: {
    from: (value) => JSON.parse(value) as JsonValue,
    to: (value) => JSON.stringify(value) ?? "null",
  },
} satisfies AppTransforms;
