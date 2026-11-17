import type { Where } from "better-auth/adapters";
import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import {
  isWhereBySingleField,
  matchesResidual,
  paginateList,
  planBetterAuthQuery,
  UnsupportedBetterAuthQueryError,
} from "./utils.js";

// Better Auth normalizes adapter filters before they reach this adapter.
// Keep fixtures at that boundary so every default is explicit.
type NormalizedWhere = Required<Where>;

describe("planBetterAuthQuery", () => {
  const isQuerySupported = (tableSchema: WasmSchema[string], where: NormalizedWhere[]) => {
    try {
      planBetterAuthQuery("t", tableSchema, where);
      return true;
    } catch (error) {
      expect(error).toBeInstanceOf(UnsupportedBetterAuthQueryError);
      return false;
    }
  };

  const tableSchema = {
    columns: [
      {
        name: "email_address",
        column_type: { type: "Text" },
        nullable: false,
      },
      {
        name: "age",
        column_type: { type: "Integer" },
        nullable: false,
      },
      {
        name: "active",
        column_type: { type: "Boolean" },
        nullable: false,
      },
      {
        name: "owner_id",
        column_type: { type: "Text" },
        nullable: true,
        references: "user",
      },
      {
        name: "payload",
        column_type: { type: "Json" },
        nullable: true,
      },
      {
        name: "bytes",
        column_type: { type: "Bytea" },
        nullable: false,
      },
      {
        name: "tags",
        column_type: { type: "Array", element: { type: "Text" } },
        nullable: false,
      },
      {
        name: "session_expires_at",
        column_type: { type: "Timestamp" },
        nullable: false,
      },
      {
        name: "last_seen_at",
        column_type: { type: "Timestamp" },
        nullable: true,
      },
      {
        name: "uuid_col",
        column_type: { type: "Uuid" },
        nullable: false,
      },
    ],
  } satisfies WasmSchema[string];

  it("supports row id comparisons and supported primitive operators", () => {
    const where: NormalizedWhere[] = [
      { field: "id", operator: "eq", value: "row-1", connector: "AND", mode: "sensitive" },
      { field: "age", operator: "gte", value: 18, connector: "AND", mode: "sensitive" },
      { field: "active", operator: "eq", value: true, connector: "AND", mode: "sensitive" },
    ];

    expect(isQuerySupported(tableSchema, where)).toBe(true);
  });

  it("supports in and not_in on columns with equality support", () => {
    expect(
      isQuerySupported(tableSchema, [
        {
          field: "id",
          operator: "in",
          value: ["row-1", "row-2"],
          connector: "AND",
          mode: "sensitive",
        },
      ]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        {
          field: "id",
          operator: "not_in",
          value: ["row-1", "row-2"],
          connector: "AND",
          mode: "sensitive",
        },
      ]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        {
          field: "email_address",
          operator: "not_in",
          value: ["blocked@example.com"],
          connector: "AND",
          mode: "sensitive",
        },
      ]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        {
          field: "owner_id",
          operator: "in",
          value: ["row-1"],
          connector: "AND",
          mode: "sensitive",
        },
      ]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        {
          field: "uuid_col",
          operator: "in",
          value: ["row-1"],
          connector: "AND",
          mode: "sensitive",
        },
      ]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        {
          field: "email_address",
          operator: "in",
          value: ["a@b.c"],
          connector: "AND",
          mode: "sensitive",
        },
      ]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        { field: "age", operator: "in", value: [18, 21], connector: "AND", mode: "sensitive" },
      ]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        { field: "active", operator: "in", value: [true], connector: "AND", mode: "sensitive" },
      ] as unknown as NormalizedWhere[]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        {
          field: "session_expires_at",
          operator: "in",
          value: [new Date("2026-01-01T00:00:00.000Z")],
          connector: "AND",
          mode: "sensitive",
        },
      ] as unknown as NormalizedWhere[]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        { field: "bytes", operator: "in", value: [[1, 2, 3]], connector: "AND", mode: "sensitive" },
      ] as unknown as NormalizedWhere[]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        { field: "tags", operator: "in", value: [["a"]], connector: "AND", mode: "sensitive" },
      ] as unknown as NormalizedWhere[]),
    ).toBe(true);
  });

  it("allows null only on nullable columns", () => {
    expect(
      isQuerySupported(tableSchema, [
        { field: "last_seen_at", operator: "eq", value: null, connector: "AND", mode: "sensitive" },
      ]),
    ).toBe(true);

    expect(
      isQuerySupported(tableSchema, [
        {
          field: "session_expires_at",
          operator: "eq",
          value: null,
          connector: "AND",
          mode: "sensitive",
        },
      ]),
    ).toBe(false);
  });

  it("rejects ne null on reference and id fields", () => {
    expect(
      isQuerySupported(tableSchema, [
        { field: "owner_id", operator: "ne", value: null, connector: "AND", mode: "sensitive" },
      ]),
    ).toBe(false);

    expect(
      isQuerySupported(tableSchema, [
        { field: "id", operator: "ne", value: null, connector: "AND", mode: "sensitive" },
      ]),
    ).toBe(false);
  });

  it("supports not-equals filters on timestamp columns", () => {
    expect(
      isQuerySupported(tableSchema, [
        {
          field: "session_expires_at",
          operator: "ne",
          value: new Date("2026-01-01T00:00:00.000Z"),
          connector: "AND",
          mode: "sensitive",
        },
      ]),
    ).toBe(true);
  });

  it("rejects OR connectors and unsupported operators", () => {
    expect(
      isQuerySupported(tableSchema, [
        { field: "age", operator: "gte", value: 18, connector: "AND", mode: "sensitive" },
        { field: "active", operator: "eq", value: true, connector: "OR", mode: "sensitive" },
      ]),
    ).toBe(false);

    expect(
      isQuerySupported(tableSchema, [
        { field: "id", operator: "contains", value: "row", connector: "AND", mode: "sensitive" },
      ]),
    ).toBe(false);
  });

  it("names the refused pattern in a typed error", () => {
    const refusal = (() => {
      try {
        planBetterAuthQuery("better_auth_user", tableSchema, [
          { field: "age", operator: "gte", value: 18, connector: "AND", mode: "sensitive" },
          { field: "active", operator: "eq", value: true, connector: "OR", mode: "sensitive" },
        ]);
      } catch (error) {
        return error as UnsupportedBetterAuthQueryError;
      }
    })();
    expect(refusal?.code).toBe("unsupported_better_auth_query");
    expect(refusal?.message).toBe(
      'Reading "better_auth_user" with an OR connector is not supported by the Jazz Better Auth adapter yet',
    );
  });

  it("ignores the connector on the first condition, which joins nothing", () => {
    expect(
      isQuerySupported(tableSchema, [
        { field: "age", operator: "gte", value: 18, connector: "OR", mode: "sensitive" },
      ]),
    ).toBe(true);
  });

  it("lowers starts_with and ends_with to contains plus an exact residual check", () => {
    const where: NormalizedWhere[] = [
      { field: "age", operator: "gte", value: 18, connector: "AND", mode: "sensitive" },
      {
        field: "email_address",
        operator: "starts_with",
        value: "ali",
        connector: "AND",
        mode: "sensitive",
      },
      {
        field: "email_address",
        operator: "ends_with",
        value: ".com",
        connector: "AND",
        mode: "sensitive",
      },
    ];

    const plan = planBetterAuthQuery("t", tableSchema, where);

    expect(plan.conditions.map(({ field, operator, value }) => [field, operator, value])).toEqual([
      ["age", "gte", 18],
      ["email_address", "contains", "ali"],
      ["email_address", "contains", ".com"],
    ]);
    expect(plan.residual).toEqual([where[1], where[2]]);
    expect(matchesResidual({ email_address: "alice@x.com" }, plan.residual)).toBe(true);
    // `contains` matches these; the residual must not.
    expect(matchesResidual({ email_address: "bali@x.com" }, plan.residual)).toBe(false);
    expect(matchesResidual({ email_address: "alice@x.com.au" }, plan.residual)).toBe(false);
    expect(matchesResidual({ email_address: null }, plan.residual)).toBe(false);
  });

  it("rejects starts_with and ends_with where contains is unsupported", () => {
    expect(
      isQuerySupported(tableSchema, [
        { field: "id", operator: "starts_with", value: "row", connector: "AND", mode: "sensitive" },
      ]),
    ).toBe(false);
    expect(
      isQuerySupported(tableSchema, [
        { field: "age", operator: "ends_with", value: "1", connector: "AND", mode: "sensitive" },
      ]),
    ).toBe(false);
  });

  it("rejects case-insensitive string matches, which the engine does not do", () => {
    expect(
      isQuerySupported(tableSchema, [
        {
          field: "email_address",
          operator: "eq",
          value: "A@B.C",
          connector: "AND",
          mode: "insensitive",
        },
      ]),
    ).toBe(false);
    expect(
      isQuerySupported(tableSchema, [
        { field: "age", operator: "eq", value: 18, connector: "AND", mode: "insensitive" },
      ]),
    ).toBe(true);
  });

  it("rejects unsupported column types", () => {
    expect(
      isQuerySupported(tableSchema, [
        { field: "uuidCol", operator: "eq", value: "row-1", connector: "AND", mode: "sensitive" },
      ]),
    ).toBe(false);
  });
});

describe("paginateList", () => {
  const testData = [
    { id: 1, name: "Alice" },
    { id: 2, name: "Bob" },
    { id: 3, name: "Charlie" },
    { id: 4, name: "David" },
    { id: 5, name: "Eve" },
    { id: 6, name: "Frank" },
    { id: 7, name: "Grace" },
    { id: 8, name: "Henry" },
  ];

  describe("basic functionality", () => {
    it("should return all data when limit and offset are undefined", () => {
      const result = paginateList(testData, undefined, undefined);
      expect(result).toEqual(testData);
    });

    it("should return all data when limit and offset are undefined (explicit)", () => {
      const result = paginateList(testData, undefined, undefined);
      expect(result).toEqual(testData);
    });

    it("should apply only limit when offset is undefined", () => {
      const result = paginateList(testData, 3, undefined);
      expect(result).toEqual([
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
        { id: 3, name: "Charlie" },
      ]);
    });

    it("should apply only offset when limit is undefined", () => {
      const result = paginateList(testData, undefined, 3);
      expect(result).toEqual([
        { id: 4, name: "David" },
        { id: 5, name: "Eve" },
        { id: 6, name: "Frank" },
        { id: 7, name: "Grace" },
        { id: 8, name: "Henry" },
      ]);
    });

    it("should apply both limit and offset", () => {
      const result = paginateList(testData, 3, 2);
      expect(result).toEqual([
        { id: 3, name: "Charlie" },
        { id: 4, name: "David" },
        { id: 5, name: "Eve" },
      ]);
    });
  });

  describe("edge cases", () => {
    it("should handle empty array", () => {
      const result = paginateList([], 5, 0);
      expect(result).toEqual([]);
    });

    it("should handle offset beyond array length", () => {
      const result = paginateList(testData, 5, 10);
      expect(result).toEqual([]);
    });

    it("should handle limit larger than remaining items", () => {
      const result = paginateList(testData, 10, 5);
      expect(result).toEqual([
        { id: 6, name: "Frank" },
        { id: 7, name: "Grace" },
        { id: 8, name: "Henry" },
      ]);
    });

    it("should handle zero limit", () => {
      const result = paginateList(testData, 0, 2);
      expect(result).toEqual([]);
    });

    it("should handle zero offset", () => {
      const result = paginateList(testData, 3, 0);
      expect(result).toEqual([
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
        { id: 3, name: "Charlie" },
      ]);
    });

    it("should handle negative offset (treats as 0)", () => {
      const result = paginateList(testData, 3, -1);
      expect(result).toEqual([
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
        { id: 3, name: "Charlie" },
      ]);
    });

    it("should handle exact boundary conditions", () => {
      const result = paginateList(testData, 4, 4);
      expect(result).toEqual([
        { id: 5, name: "Eve" },
        { id: 6, name: "Frank" },
        { id: 7, name: "Grace" },
        { id: 8, name: "Henry" },
      ]);
    });
  });

  describe("boundary testing", () => {
    it("should handle offset at array boundary", () => {
      const result = paginateList(testData, 5, 8);
      expect(result).toEqual([]);
    });

    it("should handle limit that matches remaining items exactly", () => {
      const result = paginateList(testData, 3, 5);
      expect(result).toEqual([
        { id: 6, name: "Frank" },
        { id: 7, name: "Grace" },
        { id: 8, name: "Henry" },
      ]);
    });

    it("should handle single item result", () => {
      const result = paginateList(testData, 1, 4);
      expect(result).toEqual([{ id: 5, name: "Eve" }]);
    });
  });
});

describe("isWhereBySingleField", () => {
  it("should return true if the where condition is an id", () => {
    const where: NormalizedWhere[] = [
      { field: "id", operator: "eq", value: "1", connector: "AND", mode: "sensitive" },
    ];
    expect(isWhereBySingleField("id", where)).toBe(true);
  });

  it("should return false if the where condition is not an id", () => {
    const where: NormalizedWhere[] = [
      { field: "name", operator: "eq", value: "Alice", connector: "AND", mode: "sensitive" },
    ];
    expect(isWhereBySingleField("id", where)).toBe(false);
  });

  it("should return false if the where condition has more than one condition", () => {
    const where: NormalizedWhere[] = [
      { field: "name", operator: "eq", value: "Alice", connector: "AND", mode: "sensitive" },
      { field: "id", operator: "eq", value: "1", connector: "AND", mode: "sensitive" },
    ];
    expect(isWhereBySingleField("id", where)).toBe(false);
  });

  it("should return false if the where condition has no condition", () => {
    const where: NormalizedWhere[] = [];
    expect(isWhereBySingleField("id", where)).toBe(false);
  });

  it("should return false if the where condition on ID is not eq", () => {
    const where: NormalizedWhere[] = [
      { field: "id", operator: "gt", value: "1", connector: "AND", mode: "sensitive" },
    ];
    expect(isWhereBySingleField("id", where)).toBe(false);
  });
});
