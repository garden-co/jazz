/**
 * Tests for value-converter module.
 */

import { describe, it, expect } from "vitest";
import { toValue, toValueArray, toUpdateRecord } from "./value-converter.js";
import type { WasmSchema, ColumnType } from "../drivers/types.js";

describe("toValue", () => {
  it("converts null to Null", () => {
    const colType: ColumnType = { type: "Text" };
    expect(toValue(null, colType)).toEqual({ type: "Null" });
  });

  it("converts undefined to Null", () => {
    const colType: ColumnType = { type: "Text" };
    expect(toValue(undefined, colType)).toEqual({ type: "Null" });
  });

  it("converts Text values", () => {
    const colType: ColumnType = { type: "Text" };
    expect(toValue("hello", colType)).toEqual({ type: "Text", value: "hello" });
    expect(toValue(123, colType)).toEqual({ type: "Text", value: "123" }); // coercion
  });

  it("converts Boolean values", () => {
    const colType: ColumnType = { type: "Boolean" };
    expect(toValue(true, colType)).toEqual({ type: "Boolean", value: true });
    expect(toValue(false, colType)).toEqual({ type: "Boolean", value: false });
    expect(toValue(1, colType)).toEqual({ type: "Boolean", value: true }); // coercion
    expect(toValue(0, colType)).toEqual({ type: "Boolean", value: false }); // coercion
  });

  it("converts Integer values", () => {
    const colType: ColumnType = { type: "Integer" };
    expect(toValue(42, colType)).toEqual({ type: "Integer", value: 42 });
    expect(toValue(-1, colType)).toEqual({ type: "Integer", value: -1 });
  });

  it("converts BigInt values", () => {
    const colType: ColumnType = { type: "BigInt" };
    expect(toValue(9007199254740991, colType)).toEqual({ type: "BigInt", value: 9007199254740991 });
  });

  it("converts Timestamp values", () => {
    const colType: ColumnType = { type: "Timestamp" };
    const now = Date.now();
    expect(toValue(now, colType)).toEqual({ type: "Timestamp", value: now });
  });

  it("converts Uuid values", () => {
    const colType: ColumnType = { type: "Uuid" };
    const uuid = "550e8400-e29b-41d4-a716-446655440000";
    expect(toValue(uuid, colType)).toEqual({ type: "Uuid", value: uuid });
  });

  it("converts Array values", () => {
    const colType: ColumnType = { type: "Array", element: { type: "Text" } };
    expect(toValue(["a", "b", "c"], colType)).toEqual({
      type: "Array",
      value: [
        { type: "Text", value: "a" },
        { type: "Text", value: "b" },
        { type: "Text", value: "c" },
      ],
    });
  });

  it("throws for non-array on Array type", () => {
    const colType: ColumnType = { type: "Array", element: { type: "Text" } };
    expect(() => toValue("not-an-array", colType)).toThrow("Expected array");
  });

  it("converts Row values", () => {
    const colType: ColumnType = {
      type: "Row",
      columns: [
        { name: "x", column_type: { type: "Integer" }, nullable: false },
        { name: "y", column_type: { type: "Integer" }, nullable: false },
      ],
    };
    expect(toValue({ x: 10, y: 20 }, colType)).toEqual({
      type: "Row",
      value: [
        { type: "Integer", value: 10 },
        { type: "Integer", value: 20 },
      ],
    });
  });

  it("throws for unsupported column type", () => {
    const colType = { type: "Unknown" } as unknown as ColumnType;
    expect(() => toValue("test", colType)).toThrow("Unsupported column type");
  });
});

describe("toValueArray", () => {
  const schema: WasmSchema = {
    tables: {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "priority", column_type: { type: "Integer" }, nullable: true },
        ],
      },
    },
  };

  it("converts Init object to Value array in column order", () => {
    const data = { title: "Buy milk", done: false, priority: 1 };
    const result = toValueArray(data, schema, "todos");

    expect(result).toEqual([
      { type: "Text", value: "Buy milk" },
      { type: "Boolean", value: false },
      { type: "Integer", value: 1 },
    ]);
  });

  it("handles nullable fields with null value", () => {
    const data = { title: "Buy milk", done: false, priority: null };
    const result = toValueArray(data, schema, "todos");

    expect(result).toEqual([
      { type: "Text", value: "Buy milk" },
      { type: "Boolean", value: false },
      { type: "Null" },
    ]);
  });

  it("handles nullable fields with undefined value", () => {
    const data = { title: "Buy milk", done: false };
    const result = toValueArray(data as Record<string, unknown>, schema, "todos");

    expect(result).toEqual([
      { type: "Text", value: "Buy milk" },
      { type: "Boolean", value: false },
      { type: "Null" },
    ]);
  });

  it("throws for unknown table", () => {
    expect(() => toValueArray({}, schema, "nonexistent")).toThrow('Unknown table "nonexistent"');
  });
});

describe("toUpdateRecord", () => {
  const schema: WasmSchema = {
    tables: {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "priority", column_type: { type: "Integer" }, nullable: true },
        ],
      },
    },
  };

  it("converts partial object to update record", () => {
    const data = { done: true };
    const result = toUpdateRecord(data, schema, "todos");

    expect(result).toEqual({
      done: { type: "Boolean", value: true },
    });
  });

  it("includes multiple fields", () => {
    const data = { title: "Updated title", priority: 5 };
    const result = toUpdateRecord(data, schema, "todos");

    expect(result).toEqual({
      title: { type: "Text", value: "Updated title" },
      priority: { type: "Integer", value: 5 },
    });
  });

  it("skips undefined values", () => {
    const data = { done: true, priority: undefined };
    const result = toUpdateRecord(data, schema, "todos");

    expect(result).toEqual({
      done: { type: "Boolean", value: true },
    });
    expect(result).not.toHaveProperty("priority");
  });

  it("includes null values (for clearing nullable fields)", () => {
    const data = { priority: null };
    const result = toUpdateRecord(data, schema, "todos");

    expect(result).toEqual({
      priority: { type: "Null" },
    });
  });

  it("throws for unknown column", () => {
    const data = { nonexistent: "value" };
    expect(() => toUpdateRecord(data, schema, "todos")).toThrow('Unknown column "nonexistent"');
  });

  it("throws for unknown table", () => {
    expect(() => toUpdateRecord({}, schema, "nonexistent")).toThrow('Unknown table "nonexistent"');
  });
});
