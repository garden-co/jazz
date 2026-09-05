import { describe, expect, test } from "vitest";
import { encodeRelationQueryPostcard, parseRelationQueryJsonLossless, type RelExpr } from "./ir.js";
import corpus from "../../../crates/jazz/fixtures/relation_query_postcard.json";

const filter = (literal: unknown): RelExpr => ({
  Filter: {
    input: { TableScan: { table: "t" } },
    predicate: { Cmp: { left: { column: "c" }, op: "Eq", right: { Literal: literal } } },
  },
});

describe("encodeRelationQueryPostcard", () => {
  test("matches the Rust-produced typed Postcard corpus", () => {
    for (const entry of corpus.cases) {
      const expected = Uint8Array.from(
        entry.postcard_hex.match(/../g)!.map((byte) => Number.parseInt(byte, 16)),
      );
      expect(encodeRelationQueryPostcard(entry.relation.rel as RelExpr), entry.name).toEqual(
        expected,
      );
    }
  });
  test("uses distinct typed scalar variants without a custom header", () => {
    expect([...encodeRelationQueryPostcard(filter(1))].slice(-2)).toEqual([2, 2]);
    expect([...encodeRelationQueryPostcard(filter(1.5))].slice(-10)).toEqual([
      4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xfc, 0x3f,
    ]);
    expect([...encodeRelationQueryPostcard(filter({ 猫: null, é: null }))]).not.toContain(0x4a);
  });
  test("preserves raw subscription numeric spelling", () => {
    const parse = (token: string) =>
      (
        parseRelationQueryJsonLossless(
          `{"relation_ir":{"Filter":{"input":{"TableScan":{"table":"t"}},"predicate":{"Cmp":{"left":{"column":"c"},"op":"Eq","right":{"Literal":${token}}}}}}}`,
        ) as { relation_ir: RelExpr }
      ).relation_ir;
    expect([...encodeRelationQueryPostcard(parse("1"))].slice(-2)).toEqual([2, 2]);
    expect([...encodeRelationQueryPostcard(parse("1.0"))].slice(-10)).toEqual([
      4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xf8, 0x3f,
    ]);
    expect([...encodeRelationQueryPostcard(parse("1e0"))].slice(-10)).toEqual([
      4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xf8, 0x3f,
    ]);
    expect([...encodeRelationQueryPostcard(parse("-0"))].slice(-11)).toEqual([
      4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 1,
    ]);
    expect([...encodeRelationQueryPostcard(parse("9223372036854776000"))].slice(-11)[0]).toBe(3);
    expect([...encodeRelationQueryPostcard(parse("1e21"))].slice(-10)[0]).toBe(4);
    const parseOffset = (token: string) =>
      (
        parseRelationQueryJsonLossless(
          `{"relation_ir":{"Offset":{"input":{"TableScan":{"table":"t"}},"offset":${token}}}}`,
        ) as { relation_ir: RelExpr }
      ).relation_ir;
    expect(() => encodeRelationQueryPostcard(parseOffset("1"))).not.toThrow();
    expect(() => encodeRelationQueryPostcard(parseOffset("1.0"))).toThrow("dimension");
    expect(() => encodeRelationQueryPostcard(parseOffset("-0"))).toThrow("dimension");
  });
  test("keeps numeric markers out of strings and rejects malformed UTF-16", () => {
    const parsed = parseRelationQueryJsonLossless(
      '{"relation_ir":{"Filter":{"input":{"TableScan":{"table":"t"}},"predicate":{"Cmp":{"left":{"column":"c"},"op":"Eq","right":{"Literal":[1,"__jrq_raw_number_0__"]}}}}}}',
    ) as { relation_ir: RelExpr };
    expect(() => encodeRelationQueryPostcard(parsed.relation_ir)).not.toThrow();
    expect(() =>
      parseRelationQueryJsonLossless('{"relation_ir":{"TableScan":{"table":"\\ud800"}}}'),
    ).toThrow("unpaired surrogate");
    expect(() =>
      parseRelationQueryJsonLossless(
        '{"relation_ir":{"TableScan":{"table":"t"}},"ignored":"\\ud800"}',
      ),
    ).toThrow("unpaired surrogate");
    expect(() =>
      parseRelationQueryJsonLossless('{"relation_ir":{"TableScan":{"table":"t",1:"bad"}}}'),
    ).toThrow();
  });
  test("checks byte growth before exceeding the carrier limit", () => {
    const values = Array.from({ length: 4094 }, () => ({ Param: "x".repeat(256) }) as const);
    expect(() =>
      encodeRelationQueryPostcard({
        Filter: {
          input: { TableScan: { table: "rows" } },
          predicate: { In: { left: { column: "value" }, values } },
        },
      }),
    ).toThrow("byte limit");
  });
  test("rejects unknown relation enum values and fields", () => {
    expect(() =>
      encodeRelationQueryPostcard({
        Join: {
          left: { TableScan: { table: "a" } },
          right: { TableScan: { table: "b" } },
          on: [],
          join_kind: "Cross",
        },
      } as unknown as RelExpr),
    ).toThrow("join kind");
    expect(() =>
      encodeRelationQueryPostcard({
        OrderBy: {
          input: { TableScan: { table: "a" } },
          terms: [{ column: { column: "id" }, direction: "Sideways" }],
        },
      } as unknown as RelExpr),
    ).toThrow("order direction");
    expect(() =>
      encodeRelationQueryPostcard({
        TableScan: { table: "a", unknown: true },
      } as unknown as RelExpr),
    ).toThrow("table scan");
  });
});
