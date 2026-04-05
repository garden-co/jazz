/**
 * Tests for value-converter module.
 */

import { describe, it, expect } from "vitest";
import { toInsertRecord, toValue, toUpdateRecord } from "./value-converter.js";
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

  it("converts Date objects for Timestamp columns", () => {
    const colType: ColumnType = { type: "Timestamp" };
    const ts = 1704067200000;
    const date = new Date(ts);
    expect(toValue(date, colType)).toEqual({ type: "Timestamp", value: ts });
  });

  it("throws for invalid Date in Timestamp columns", () => {
    const colType: ColumnType = { type: "Timestamp" };
    expect(() => toValue(new Date("not-a-date"), colType)).toThrow("Invalid timestamp value");
  });

  it("converts Uuid values", () => {
    const colType: ColumnType = { type: "Uuid" };
    const uuid = "550e8400-e29b-41d4-a716-446655440000";
    expect(toValue(uuid, colType)).toEqual({ type: "Uuid", value: uuid });
  });

  it("converts Bytea Uint8Array values", () => {
    const colType: ColumnType = { type: "Bytea" };
    const bytes = new Uint8Array([0, 10, 255]);
    const converted = toValue(bytes, colType);
    expect(converted.type).toBe("Bytea");
    if (converted.type !== "Bytea") {
      throw new Error("expected Bytea value");
    }
    expect(converted.value).toBeInstanceOf(Uint8Array);
    expect(Array.from(converted.value)).toEqual([0, 10, 255]);
  });

  it("converts Bytea arrays to Uint8Array values", () => {
    const colType: ColumnType = { type: "Bytea" };
    const converted = toValue([0, 10, 255], colType);
    expect(converted.type).toBe("Bytea");
    if (converted.type !== "Bytea") {
      throw new Error("expected Bytea value");
    }
    expect(converted.value).toBeInstanceOf(Uint8Array);
    expect(Array.from(converted.value)).toEqual([0, 10, 255]);
  });

  it("rejects invalid Bytea values", () => {
    const colType: ColumnType = { type: "Bytea" };
    expect(() => toValue("abc", colType)).toThrow("Expected Uint8Array or byte array");
    expect(() => toValue([0, 256], colType)).toThrow("Bytea arrays must contain integers");
  });

  it("converts Json values", () => {
    const colType: ColumnType = { type: "Json" };
    expect(toValue('{"a":1}', colType)).toEqual({ type: "Text", value: '{"a":1}' });
    expect(toValue({ a: 1, b: ["x"] }, colType)).toEqual({
      type: "Text",
      value: '{"a":1,"b":["x"]}',
    });
  });

  it("rejects non-serializable Json values", () => {
    const colType: ColumnType = { type: "Json" };
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    expect(() => toValue(circular, colType)).toThrow("JSON values must be serializable");
  });

  it("converts Enum values and validates variants", () => {
    const colType = { type: "Enum", variants: ["done", "todo"] } as ColumnType;
    expect(toValue("todo", colType)).toEqual({ type: "Text", value: "todo" });
    expect(() => toValue("invalid", colType)).toThrow("Invalid enum value");
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
      value: {
        values: [
          { type: "Integer", value: 10 },
          { type: "Integer", value: 20 },
        ],
      },
    });
  });

  it("converts Double values", () => {
    const colType: ColumnType = { type: "Double" };
    expect(toValue(23.456, colType)).toEqual({ type: "Double", value: 23.456 });
    expect(toValue(-0.001, colType)).toEqual({ type: "Double", value: -0.001 });
    expect(toValue(0, colType)).toEqual({ type: "Double", value: 0 });
  });

  it("throws for unsupported column type", () => {
    const colType = { type: "Unknown" } as unknown as ColumnType;
    expect(() => toValue("test", colType)).toThrow("Unsupported column type");
  });
});

describe("toInsertRecord", () => {
  const schema: WasmSchema = {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
        { name: "priority", column_type: { type: "Integer" }, nullable: true },
        { name: "payload", column_type: { type: "Bytea" }, nullable: true },
      ],
    },
  };

  it("converts Init object to a named insert record", () => {
    const data = { title: "Buy milk", done: false, priority: 1 };
    const result = toInsertRecord(data, schema, "todos");

    expect(result).toEqual({
      title: { type: "Text", value: "Buy milk" },
      done: { type: "Boolean", value: false },
      priority: { type: "Integer", value: 1 },
    });
  });

  it("includes nullable fields with null value", () => {
    const data = { title: "Buy milk", done: false, priority: null };
    const result = toInsertRecord(data, schema, "todos");

    expect(result).toEqual({
      title: { type: "Text", value: "Buy milk" },
      done: { type: "Boolean", value: false },
      priority: { type: "Null" },
    });
  });

  it("skips undefined fields so Rust can apply defaults", () => {
    const data = { title: "Buy milk", done: false };
    const result = toInsertRecord(data as Record<string, unknown>, schema, "todos");

    expect(result).toEqual({
      title: { type: "Text", value: "Buy milk" },
      done: { type: "Boolean", value: false },
    });
    expect(result).not.toHaveProperty("priority");
  });

  it("normalizes Bytea inserts to JSON-friendly byte arrays", () => {
    const data = {
      title: "Buy milk",
      done: false,
      payload: new Uint8Array([1, 2, 3]),
    };

    const result = toInsertRecord(data, schema, "todos");

    expect(result.payload?.type).toBe("Bytea");
    expect(result.payload?.value).toBeInstanceOf(Uint8Array);
    expect(Array.from((result.payload as { type: "Bytea"; value: Uint8Array }).value)).toEqual([
      1, 2, 3,
    ]);
  });

  it("throws for unknown table", () => {
    expect(() => toInsertRecord({}, schema, "nonexistent")).toThrow('Unknown table "nonexistent"');
  });

  it("throws for unknown column", () => {
    expect(() => toInsertRecord({ nope: 1 }, schema, "todos")).toThrow(
      'Unknown column "nope" on table "todos"',
    );
  });

  it("throws when null is used for a required field", () => {
    expect(() => toInsertRecord({ title: null }, schema, "todos")).toThrow(
      "Cannot set required field 'title' to null",
    );
  });
});

describe("toUpdateRecord", () => {
  const schema: WasmSchema = {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
        { name: "priority", column_type: { type: "Integer" }, nullable: true },
        { name: "payload", column_type: { type: "Bytea" }, nullable: true },
      ],
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

  it("normalizes Bytea updates to JSON-friendly byte arrays", () => {
    const data = { payload: new Uint8Array([4, 5, 6]) };
    const result = toUpdateRecord(data, schema, "todos");

    expect(result.payload?.type).toBe("Bytea");
    expect(result.payload?.value).toBeInstanceOf(Uint8Array);
    expect(Array.from((result.payload as { type: "Bytea"; value: Uint8Array }).value)).toEqual([
      4, 5, 6,
    ]);
  });

  it("throws when null is used to unset a required field", () => {
    const data = { title: null };
    expect(() => toUpdateRecord(data, schema, "todos")).toThrow(
      "Cannot set required field 'title' to null",
    );
  });

  it("throws for unknown column", () => {
    const data = { nonexistent: "value" };
    expect(() => toUpdateRecord(data, schema, "todos")).toThrow('Unknown column "nonexistent"');
  });

  it("throws for unknown table", () => {
    expect(() => toUpdateRecord({}, schema, "nonexistent")).toThrow('Unknown table "nonexistent"');
  });
});
