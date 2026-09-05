import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { queryWithPredicates, type QueryArraySubquery, type QueryOrder } from "./native-codec.js";

type NativeQueryCodecFixture = {
  cases: Array<{ name: string; query_hex: string }>;
};

describe("native query codec", () => {
  it("encodes a payload enum match as the core predicate with a nested payload tree", () => {
    expect(
      bytesToHex(
        queryWithPredicates("events", [
          {
            column: "event",
            op: "EnumMatch",
            case: "message",
            payload: {
              op: "All",
              predicates: [{ column: "level", op: "Eq", value: { type: "Integer", value: 2 } }],
            },
          },
        ]),
      ),
    ).toBe(
      "066576656e7473010b00056576656e74076d65737361676500010300056c6576656c030f0400000000000000000000000000",
    );
  });

  it("pins forward, reverse, nested, projected, and required relation query layouts", () => {
    const fixture = nativeQueryCodecFixture();
    for (const [name, table, arraySubqueries, select, orderBy, relation] of queryCases()) {
      const expected = fixture.cases.find((candidate) => candidate.name === name);
      expect(expected, `${name} fixture is present`).toBeDefined();
      expect(
        bytesToHex(queryWithPredicates(table, [], { arraySubqueries, select, orderBy, relation })),
      ).toBe(expected!.query_hex);
    }
  });

  it.each([
    ["limit", -1],
    ["limit", Number.NaN],
    ["limit", 1.5],
    ["limit", Number.MAX_SAFE_INTEGER + 1],
    ["offset", -1],
    ["offset", Number.NaN],
    ["offset", 1.5],
    ["offset", Number.MAX_SAFE_INTEGER + 1],
  ] as const)("rejects an invalid array subquery %s of %s", (field, value) => {
    expect(() =>
      queryWithPredicates("teams", [], {
        arraySubqueries: [
          {
            columnName: "participants",
            table: "participants",
            innerColumn: "team_id",
            outerColumn: "id",
            limit: field === "limit" ? value : 2,
            offset: field === "offset" ? value : 0,
          },
        ],
      }),
    ).toThrow(`array subquery participants ${field} must be a non-negative safe integer`);
  });
});

function queryCases(): Array<
  [string, string, QueryArraySubquery[], string[] | undefined, QueryOrder[], unknown?]
> {
  return [
    [
      "forward_include_projected_optional",
      "accounts",
      [
        {
          columnName: "entries",
          table: "entries",
          innerColumn: "account_id",
          outerColumn: "id",
          select: ["label"],
          orderBy: [{ column: "label", direction: "Asc" }],
          limit: 3,
          offset: 1,
        },
      ],
      ["label"],
      [{ column: "label", direction: "Asc" }],
    ],
    [
      "reverse_include_required_nested_projection",
      "groups",
      [
        {
          columnName: "members",
          table: "members",
          innerColumn: "group_id",
          outerColumn: "id",
          filters: [{ column: "state", op: "Eq", value: { type: "Text", value: "active" } }],
          select: ["name"],
          limit: 4,
          requirement: "AtLeastOne",
          nestedArrays: [
            {
              columnName: "notes",
              table: "notes",
              innerColumn: "member_id",
              outerColumn: "id",
              select: ["body"],
              limit: 2,
              requirement: "MatchCorrelationCardinality",
            },
          ],
        },
      ],
      undefined,
      [],
    ],
    [
      "unbounded_reverse_include_with_offset",
      "teams",
      [
        {
          columnName: "participants",
          table: "participants",
          innerColumn: "team_id",
          outerColumn: "id",
          offset: 2,
        },
      ],
      undefined,
      [],
    ],
    [
      "labeled_union_relation_json_literal",
      "todos",
      [],
      undefined,
      [],
      {
        Limit: {
          input: {
            Union: {
              inputs: [
                { label: "first", input: { TableScan: { table: "todos", alias: "left" } } },
                {
                  label: "second",
                  input: {
                    Filter: {
                      input: { TableScan: { table: "todos" } },
                      predicate: {
                        Cmp: {
                          left: { scope: "todos", column: "metadata" },
                          op: "Eq",
                          right: { Literal: { type: "Json", value: { nested: [true, null, 7] } } },
                        },
                      },
                    },
                  },
                },
              ],
            },
          },
          limit: 1,
        },
      },
    ],
  ];
}

function nativeQueryCodecFixture(): NativeQueryCodecFixture {
  return JSON.parse(
    readFileSync(
      new URL("../../../../../crates/jazz/fixtures/native_query_codec.json", import.meta.url),
      "utf8",
    ),
  ) as NativeQueryCodecFixture;
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
