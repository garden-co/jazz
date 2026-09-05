import { describe, expect, test } from "vitest";
import { encodeRelationQueryV1, type RelExpr } from "./ir.js";
import corpus from "../../../crates/jazz/fixtures/relation_query_jrq_v1.json";

const filter = (literal: unknown): RelExpr => ({
  Filter: {
    input: { TableScan: { table: "t" } },
    predicate: { Cmp: { left: { column: "c" }, op: "Eq", right: { Literal: literal } } },
  },
});

describe("encodeRelationQueryV1", () => {
  test("matches the Rust-produced all-expression corpus", () => {
    for (const entry of corpus.cases) {
      const expected = Uint8Array.from(
        entry.jrq_hex.match(/../g)!.map((byte) => Number.parseInt(byte, 16)),
      );
      expect(encodeRelationQueryV1(entry.relation.rel as RelExpr), entry.name).toEqual(expected);
    }
  });
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
    expect([...encodeRelationQueryV1(filter({ 猫: null, é: null }))]).toEqual([
      0x4a, 0x52, 0x51, 0x01, 1, 0, 1, 0x74, 0, 0, 0, 1, 0x63, 0, 0, 8, 2, 2, 0xc3, 0xa9, 0, 3,
      0xe7, 0x8c, 0xab, 0,
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

  test("preserves JSON decimal normalization for unsafe integers and rejects nonportable dimensions", () => {
    expect([...encodeRelationQueryV1(filter(2 ** 63))][15]).toBe(4);
    expect([...encodeRelationQueryV1(filter(2 ** 64))][15]).toBe(5);
    expect(() =>
      encodeRelationQueryV1({
        Offset: { input: { TableScan: { table: "t" } }, offset: 0x1_0000_0000 },
      }),
    ).toThrow("dimension");
  });

  test("rejects unpaired UTF-16 surrogates before TextEncoder normalization", () => {
    expect(() => encodeRelationQueryV1({ TableScan: { table: "\ud800" } })).toThrow(
      "unpaired surrogate",
    );
  });

  test("checked writer rejects mixed values at the byte boundary", () => {
    const values = [
      ...Array.from({ length: 3000 }, () => ({ Param: "x".repeat(346) }) as const),
      ...Array.from({ length: 1095 }, () => ({ RowId: "Current" }) as const),
    ];
    expect(() =>
      encodeRelationQueryV1({
        Filter: {
          input: { TableScan: { table: "rows" } },
          predicate: { In: { left: { column: "value" }, values } },
        },
      }),
    ).toThrow("byte limit");
  });
});
