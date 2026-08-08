import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { queryWithPredicates, type QueryArraySubquery, type QueryOrder } from "./native-codec.js";

type NativeQueryCodecFixture = {
  cases: Array<{ name: string; query_hex: string }>;
};

describe("native query codec", () => {
  it("pins forward, reverse, nested, projected, and required relation query layouts", () => {
    const fixture = nativeQueryCodecFixture();
    for (const [name, table, arraySubqueries, select, orderBy] of queryCases()) {
      const expected = fixture.cases.find((candidate) => candidate.name === name);
      expect(expected, `${name} fixture is present`).toBeDefined();
      expect(bytesToHex(queryWithPredicates(table, [], { arraySubqueries, select, orderBy }))).toBe(
        expected!.query_hex,
      );
    }
  });

  it("rejects an array subquery whose bound intent was lost", () => {
    expect(() =>
      queryWithPredicates("teams", [], {
        arraySubqueries: [
          {
            columnName: "participants",
            table: "participants",
            innerColumn: "team_id",
            outerColumn: "id",
          },
        ],
      }),
    ).toThrow("array subquery participants must specify limit or explicitly declare unbounded");
  });

  it("rejects an array subquery declaring finite and unbounded together", () => {
    expect(() =>
      queryWithPredicates("teams", [], {
        arraySubqueries: [
          {
            columnName: "participants",
            table: "participants",
            innerColumn: "team_id",
            outerColumn: "id",
            limit: 2,
            unbounded: true,
          },
        ],
      }),
    ).toThrow("array subquery participants cannot specify both limit and unbounded");
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
  [string, string, QueryArraySubquery[], string[] | undefined, QueryOrder[]]
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
          unbounded: true,
          offset: 2,
        },
      ],
      undefined,
      [],
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
