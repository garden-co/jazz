import { describe, expect, it } from "vitest";
import { decodeRow } from "../backend/api-data.js";
import { stringifyForPresentation } from "./presentation-serialization.js";

// #5202: the backend summarizes a value too large to list as
// { $large: { kind, size, preview } }; the grid shows what it is.

const photo = { $large: { kind: "bytes" as const, size: 5_452_595, preview: "iVBORw0K" } };

describe("a large value's summary", () => {
  it("is shown as its kind and size", () => {
    expect(stringifyForPresentation(photo)).toBe("‹large bytes value, 5.2 MB›");
    expect(
      stringifyForPresentation({ $large: { kind: "text", size: 200_000, preview: "x" } }),
    ).toBe("‹large text value, 200,000 characters›");
  });

  it("is kept as is when a row is decoded, not read as the column's value", () => {
    const columns = new Map([
      ["photo", { name: "photo", column_type: { type: "Bytea" }, nullable: false } as never],
    ]);
    expect(decodeRow(columns, { id: "row-1", photo })).toEqual({ id: "row-1", photo });
  });
});
