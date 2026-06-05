import { describe, expect, it } from "vitest";
import {
  basicSchema,
  expectFilterPredicate,
  parseTranslatedQuery,
  translateQuery,
} from "./support.js";

describe("condition translation", () => {
  it("translates eq condition with string", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "title", op: "eq", value: "Buy milk" }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "title" },
      op: "Eq",
      right: { type: "Literal", value: { Text: "Buy milk" } },
    });
  });

  it("translates eq condition with provenance magic text columns", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "$createdBy", op: "eq", value: "alice" }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "$createdBy" },
      op: "Eq",
      right: { type: "Literal", value: { Text: "alice" } },
    });
  });

  it("translates eq condition with enum value", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "status", op: "eq", value: "todo" }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "status" },
      op: "Eq",
      right: { type: "Literal", value: { Text: "todo" } },
    });
  });

  it("translates eq condition with UUID string for Uuid columns", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "project", op: "eq", value: "00000000-0000-0000-0000-000000000123" }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "project" },
      op: "Eq",
      right: {
        type: "Literal",
        value: { Uuid: "00000000-0000-0000-0000-000000000123" },
      },
    });
  });

  it("treats implicit id column as UUID", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "id", op: "eq", value: "00000000-0000-0000-0000-000000000abc" }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "id" },
      op: "Eq",
      right: {
        type: "Literal",
        value: { Uuid: "00000000-0000-0000-0000-000000000abc" },
      },
    });
  });

  it("translates eq condition with boolean", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "done", op: "eq", value: false }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "done" },
      op: "Eq",
      right: { type: "Literal", value: { Boolean: false } },
    });
  });

  it("translates eq condition with magic boolean column", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "$canEdit", op: "eq", value: true }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "$canEdit" },
      op: "Eq",
      right: { type: "Literal", value: { Boolean: true } },
    });
  });

  it("translates eq condition with number", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "eq", value: 5 }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "priority" },
      op: "Eq",
      right: { type: "Literal", value: { Integer: 5 } },
    });
  });

  it("translates eq condition with number for Timestamp columns", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "created_at", op: "eq", value: 1712345678 }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "created_at" },
      op: "Eq",
      right: { type: "Literal", value: { Timestamp: 1712345678 } },
    });
  });

  it("translates eq condition with ISO string for Timestamp columns", () => {
    const iso = "2024-01-01T00:00:00.000Z";
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "created_at", op: "eq", value: iso }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "created_at" },
      op: "Eq",
      right: { type: "Literal", value: { Timestamp: Date.parse(iso) } },
    });
  });

  it("throws for invalid timestamp string condition", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "created_at", op: "eq", value: "not-a-date" }],
      includes: {},
      orderBy: [],
    });

    expect(() => parseTranslatedQuery(builderJson, basicSchema)).toThrow(
      "Invalid timestamp condition",
    );
  });

  it("translates numeric string for Timestamp columns as epoch number", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "created_at", op: "eq", value: "1712345678" }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "created_at" },
      op: "Eq",
      right: { type: "Literal", value: { Timestamp: 1712345678 } },
    });
  });

  it("translates eq condition with array value", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [
        {
          column: "tags",
          op: "eq",
          value: ["tag1", "tag2"],
        },
      ],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "tags" },
      op: "Eq",
      right: {
        type: "Literal",
        value: {
          Array: [{ Text: "tag1" }, { Text: "tag2" }],
        },
      },
    });
  });

  it("translates eq condition with Json object value", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "metadata", op: "eq", value: { phase: "alpha", retries: 1 } }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "metadata" },
      op: "Eq",
      right: {
        type: "Literal",
        value: { Text: '{"phase":"alpha","retries":1}' },
      },
    });
  });

  it("translates in condition with Json values", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [
        {
          column: "metadata",
          op: "in",
          value: [{ phase: "alpha" }, { phase: "beta" }],
        },
      ],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "In",
      left: { scope: "todos", column: "metadata" },
      values: [
        { type: "Literal", value: { Text: '{"phase":"alpha"}' } },
        { type: "Literal", value: { Text: '{"phase":"beta"}' } },
      ],
    });
  });

  it("translates contains condition with array element value", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [
        {
          column: "tags",
          op: "contains",
          value: "tag1",
        },
      ],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Contains",
      left: { scope: "todos", column: "tags" },
      value: { type: "Literal", value: { Text: "tag1" } },
    });
  });

  it("translates contains condition with non-text array element values", () => {
    const reviewedAt = "2026-02-03T04:05:06.000Z";
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [
        { column: "checkpoints", op: "contains", value: 3 },
        { column: "flags", op: "contains", value: true },
        {
          column: "reviewers",
          op: "contains",
          value: "00000000-0000-0000-0000-000000000123",
        },
        { column: "status_history", op: "contains", value: "done" },
        { column: "reviewed_at", op: "contains", value: reviewedAt },
      ],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "And",
      exprs: [
        {
          type: "Contains",
          left: { scope: "todos", column: "checkpoints" },
          value: { type: "Literal", value: { Integer: 3 } },
        },
        {
          type: "Contains",
          left: { scope: "todos", column: "flags" },
          value: { type: "Literal", value: { Boolean: true } },
        },
        {
          type: "Contains",
          left: { scope: "todos", column: "reviewers" },
          value: {
            type: "Literal",
            value: { Uuid: "00000000-0000-0000-0000-000000000123" },
          },
        },
        {
          type: "Contains",
          left: { scope: "todos", column: "status_history" },
          value: { type: "Literal", value: { Text: "done" } },
        },
        {
          type: "Contains",
          left: { scope: "todos", column: "reviewed_at" },
          value: { type: "Literal", value: { Timestamp: Date.parse(reviewedAt) } },
        },
      ],
    });
  });

  it("translates ne condition", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "done", op: "ne", value: true }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "done" },
      op: "Ne",
      right: { type: "Literal", value: { Boolean: true } },
    });
  });

  it("translates gt condition", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "gt", value: 3 }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "priority" },
      op: "Gt",
      right: { type: "Literal", value: { Integer: 3 } },
    });
  });

  it("translates gte condition", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "gte", value: 3 }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "priority" },
      op: "Ge",
      right: { type: "Literal", value: { Integer: 3 } },
    });
  });

  it("translates gte condition with provenance magic timestamp columns", () => {
    const iso = "2026-03-31T00:00:00.000Z";
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "$updatedAt", op: "gte", value: iso }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "$updatedAt" },
      op: "Ge",
      right: { type: "Literal", value: { Timestamp: Date.parse(iso) * 1_000 } },
    });
  });

  it("translates lt condition", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "lt", value: 3 }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "priority" },
      op: "Lt",
      right: { type: "Literal", value: { Integer: 3 } },
    });
  });

  it("translates lte condition", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "lte", value: 3 }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "priority" },
      op: "Le",
      right: { type: "Literal", value: { Integer: 3 } },
    });
  });

  it("translates isNull condition", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "isNull", value: true }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "IsNull",
      column: { scope: "todos", column: "priority" },
    });
  });

  it("translates isNull=false condition to IsNotNull", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "isNull", value: false }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "IsNotNull",
      column: { scope: "todos", column: "priority" },
    });
  });

  it("rejects non-boolean values for isNull condition", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "isNull", value: "false" }],
      includes: {},
      orderBy: [],
    });

    expect(() => translateQuery(builderJson, basicSchema)).toThrow(
      '"isNull" operator requires a boolean value.',
    );
  });

  it("translates multiple conditions", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [
        { column: "done", op: "eq", value: false },
        { column: "priority", op: "gt", value: 3 },
      ],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "And",
      exprs: [
        {
          type: "Cmp",
          left: { scope: "todos", column: "done" },
          op: "Eq",
          right: { type: "Literal", value: { Boolean: false } },
        },
        {
          type: "Cmp",
          left: { scope: "todos", column: "priority" },
          op: "Gt",
          right: { type: "Literal", value: { Integer: 3 } },
        },
      ],
    });
  });

  it("translates eq null value to IsNull", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "eq", value: null }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "IsNull",
      column: { scope: "todos", column: "priority" },
    });
  });

  it("translates ne null value to IsNotNull", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "priority", op: "ne", value: null }],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(expectFilterPredicate(result)).toEqual({
      type: "IsNotNull",
      column: { scope: "todos", column: "priority" },
    });
  });

  it("throws for unknown operator", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "done", op: "unknown", value: true }],
      includes: {},
      orderBy: [],
    });

    expect(() => translateQuery(builderJson, basicSchema)).toThrow("Unknown operator: unknown");
  });

  it("throws for invalid enum value", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "status", op: "eq", value: "invalid" }],
      includes: {},
      orderBy: [],
    });

    expect(() => translateQuery(builderJson, basicSchema)).toThrow("Invalid enum value");
  });

  it("rejects unsupported Json comparison operators", () => {
    const gtBuilderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "metadata", op: "gt", value: { retries: 1 } }],
      includes: {},
      orderBy: [],
    });
    expect(() => translateQuery(gtBuilderJson, basicSchema)).toThrow(
      'JSON column "metadata" only supports eq/ne/in/isNull operators.',
    );

    const containsBuilderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "metadata", op: "contains", value: { retries: 1 } }],
      includes: {},
      orderBy: [],
    });
    expect(() => translateQuery(containsBuilderJson, basicSchema)).toThrow(
      'JSON column "metadata" only supports eq/ne/in/isNull operators.',
    );
  });
});
