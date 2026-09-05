import { describe, expect, test } from "vitest";
import { encodeRelationQueryV1, type RelExpr } from "./ir.js";

const filter = (literal: unknown): RelExpr => ({
  Filter: {
    input: { TableScan: { table: "t" } },
    predicate: { Cmp: { left: { column: "c" }, op: "Eq", right: { Literal: literal } } },
  },
});

describe("encodeRelationQueryV1", () => {
  test("matches Rust's canonical integer and raw-f64 literal vectors", () => {
    expect([...encodeRelationQueryV1(filter(1))]).toEqual([
      0x4a, 0x52, 0x51, 0x01, 1, 0, 1, 0x74, 0, 0, 0, 1, 0x63, 0, 0, 3, 2,
    ]);
    expect([...encodeRelationQueryV1(filter(-1))]).toEqual([
      0x4a, 0x52, 0x51, 0x01, 1, 0, 1, 0x74, 0, 0, 0, 1, 0x63, 0, 0, 3, 1,
    ]);
    expect([...encodeRelationQueryV1(filter(1.5))]).toEqual([
      0x4a, 0x52, 0x51, 0x01, 1, 0, 1, 0x74, 0, 0, 0, 1, 0x63, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0xf8,
      0x3f,
    ]);
  });

  test("rejects ambiguous and unknown public relation variants", () => {
    expect(() =>
      encodeRelationQueryV1({
        TableScan: { table: "t" },
        Limit: { input: { TableScan: { table: "t" } }, limit: 1 },
      } as unknown as RelExpr),
    ).toThrow("expression");
    expect(() => encodeRelationQueryV1({ Unknown: {} } as unknown as RelExpr)).toThrow(
      "expression",
    );
  });
});
