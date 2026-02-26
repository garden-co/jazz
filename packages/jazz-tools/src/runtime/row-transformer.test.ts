/**
 * Tests for row-transformer.
 */

import { describe, it, expect } from "vitest";
import { unwrapValue, transformRows, type WasmValue } from "./row-transformer.js";
import type { WasmSchema, WasmRow } from "../drivers/types.js";

describe("unwrapValue", () => {
  it("unwraps Text to string", () => {
    const v: WasmValue = { type: "Text", value: "hello" };
    expect(unwrapValue(v)).toBe("hello");
  });

  it("unwraps Uuid to string", () => {
    const v: WasmValue = { type: "Uuid", value: "abc-123" };
    expect(unwrapValue(v)).toBe("abc-123");
  });

  it("unwraps Boolean to boolean", () => {
    expect(unwrapValue({ type: "Boolean", value: true })).toBe(true);
    expect(unwrapValue({ type: "Boolean", value: false })).toBe(false);
  });

  it("unwraps Integer to number", () => {
    const v: WasmValue = { type: "Integer", value: 42 };
    expect(unwrapValue(v)).toBe(42);
  });

  it("unwraps BigInt to number", () => {
    const v: WasmValue = { type: "BigInt", value: 9007199254740991 };
    expect(unwrapValue(v)).toBe(9007199254740991);
  });

  it("unwraps Timestamp to Date", () => {
    const v: WasmValue = { type: "Timestamp", value: 1704067200000 };
    const result = unwrapValue(v);
    expect(result).toBeInstanceOf(Date);
    expect((result as Date).getTime()).toBe(1704067200000);
  });

  it("unwraps Bytea to Uint8Array", () => {
    const v: WasmValue = { type: "Bytea", value: new Uint8Array([0, 1, 255]) };
    const unwrapped = unwrapValue(v);
    expect(unwrapped).toBeInstanceOf(Uint8Array);
    expect(Array.from(unwrapped as Uint8Array)).toEqual([0, 1, 255]);
  });

  it("unwraps Null to undefined", () => {
    const v: WasmValue = { type: "Null" };
    expect(unwrapValue(v)).toBeUndefined();
  });

  it("unwraps Array recursively", () => {
    const v: WasmValue = {
      type: "Array",
      value: [
        { type: "Text", value: "a" },
        { type: "Integer", value: 1 },
      ],
    };
    expect(unwrapValue(v)).toEqual(["a", 1]);
  });

  it("unwraps Row recursively", () => {
    const v: WasmValue = {
      type: "Row",
      value: [
        { type: "Text", value: "cell1" },
        { type: "Boolean", value: true },
      ],
    };
    expect(unwrapValue(v)).toEqual(["cell1", true]);
  });

  it("handles nested arrays", () => {
    const v: WasmValue = {
      type: "Array",
      value: [
        {
          type: "Array",
          value: [
            { type: "Integer", value: 1 },
            { type: "Integer", value: 2 },
          ],
        },
        {
          type: "Array",
          value: [
            { type: "Integer", value: 3 },
            { type: "Integer", value: 4 },
          ],
        },
      ],
    };
    expect(unwrapValue(v)).toEqual([
      [1, 2],
      [3, 4],
    ]);
  });
});

describe("transformRows", () => {
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

  const relationSchema: WasmSchema = {
    tables: {
      users: {
        columns: [
          { name: "name", column_type: { type: "Text" }, nullable: false },
          {
            name: "manager_id",
            column_type: { type: "Uuid" },
            nullable: true,
            references: "users",
          },
        ],
      },
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "owner_id", column_type: { type: "Uuid" }, nullable: false, references: "users" },
        ],
      },
    },
  };

  it("transforms rows to typed objects with id", () => {
    const rows: WasmRow[] = [
      {
        id: "uuid-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
          { type: "Integer", value: 5 },
        ],
      },
    ];

    const result = transformRows<{ id: string; title: string; done: boolean; priority: number }>(
      rows,
      schema,
      "todos",
    );

    expect(result).toEqual([
      {
        id: "uuid-1",
        title: "Buy milk",
        done: false,
        priority: 5,
      },
    ]);
  });

  it("transforms multiple rows", () => {
    const rows: WasmRow[] = [
      {
        id: "uuid-1",
        values: [
          { type: "Text", value: "Task 1" },
          { type: "Boolean", value: true },
          { type: "Null" },
        ],
      },
      {
        id: "uuid-2",
        values: [
          { type: "Text", value: "Task 2" },
          { type: "Boolean", value: false },
          { type: "Integer", value: 3 },
        ],
      },
    ];

    const result = transformRows(rows, schema, "todos");

    expect(result).toHaveLength(2);
    expect(result[0]).toMatchObject({ id: "uuid-1", title: "Task 1", done: true });
    expect(result[1]).toMatchObject({ id: "uuid-2", title: "Task 2", done: false, priority: 3 });
  });

  it("handles null values", () => {
    const rows: WasmRow[] = [
      {
        id: "uuid-1",
        values: [
          { type: "Text", value: "Test" },
          { type: "Boolean", value: false },
          { type: "Null" },
        ],
      },
    ];

    const result = transformRows<{ id: string; title: string; done: boolean; priority?: number }>(
      rows,
      schema,
      "todos",
    );

    expect(result[0].priority).toBeUndefined();
  });

  it("throws for unknown table", () => {
    expect(() => transformRows([], schema, "nonexistent")).toThrow(
      'Unknown table "nonexistent" in schema',
    );
  });

  it("handles empty rows array", () => {
    const result = transformRows([], schema, "todos");
    expect(result).toEqual([]);
  });

  it("transforms timestamp values to Date objects", () => {
    const timestampSchema: WasmSchema = {
      tables: {
        events: {
          columns: [{ name: "created_at", column_type: { type: "Timestamp" }, nullable: false }],
        },
      },
    };
    const ts = 1704067200000;
    const rows: WasmRow[] = [
      {
        id: "event-1",
        values: [{ type: "Timestamp", value: ts }],
      },
    ];

    const result = transformRows<{ id: string; created_at: Date }>(rows, timestampSchema, "events");
    expect(result[0]?.created_at).toBeInstanceOf(Date);
    expect(result[0]?.created_at.getTime()).toBe(ts);
  });

  it("follows schema column order", () => {
    // Even if WASM returns values in a different order conceptually,
    // we map them based on positional index matching schema column order
    const customSchema: WasmSchema = {
      tables: {
        items: {
          columns: [
            { name: "first", column_type: { type: "Text" }, nullable: false },
            { name: "second", column_type: { type: "Integer" }, nullable: false },
            { name: "third", column_type: { type: "Boolean" }, nullable: false },
          ],
        },
      },
    };

    const rows: WasmRow[] = [
      {
        id: "id-1",
        values: [
          { type: "Text", value: "A" },
          { type: "Integer", value: 2 },
          { type: "Boolean", value: true },
        ],
      },
    ];

    const result = transformRows(rows, customSchema, "items");

    expect(result[0]).toEqual({
      id: "id-1",
      first: "A",
      second: 2,
      third: true,
    });
  });

  it("maps forward include arrays to relation names", () => {
    const rows: WasmRow[] = [
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Uuid", value: "user-1" },
          {
            type: "Array",
            value: [
              {
                type: "Row",
                value: [{ type: "Text", value: "Alice" }, { type: "Null" }],
              },
            ],
          },
        ],
      },
    ];

    const result = transformRows(rows, relationSchema, "todos", { owner: true });

    expect(result).toEqual([
      {
        id: "todo-1",
        title: "Buy milk",
        owner_id: "user-1",
        owner: {
          name: "Alice",
          manager_id: undefined,
        },
      },
    ]);
  });

  it("maps reverse include arrays to relation names", () => {
    const rows: WasmRow[] = [
      {
        id: "user-1",
        values: [
          { type: "Text", value: "Alice" },
          { type: "Null" },
          {
            type: "Array",
            value: [
              {
                type: "Row",
                value: [
                  { type: "Text", value: "Buy milk" },
                  { type: "Uuid", value: "user-1" },
                ],
              },
              {
                type: "Row",
                value: [
                  { type: "Text", value: "Write tests" },
                  { type: "Uuid", value: "user-1" },
                ],
              },
            ],
          },
        ],
      },
    ];

    const result = transformRows(rows, relationSchema, "users", { todosViaOwner: true });

    expect(result).toEqual([
      {
        id: "user-1",
        name: "Alice",
        manager_id: undefined,
        todosViaOwner: [
          { title: "Buy milk", owner_id: "user-1" },
          { title: "Write tests", owner_id: "user-1" },
        ],
      },
    ]);
  });

  it("maps nested includes recursively", () => {
    const rows: WasmRow[] = [
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Uuid", value: "user-1" },
          {
            type: "Array",
            value: [
              {
                type: "Row",
                value: [
                  { type: "Text", value: "Alice" },
                  { type: "Uuid", value: "user-2" },
                  {
                    type: "Array",
                    value: [
                      {
                        type: "Row",
                        value: [{ type: "Text", value: "Manager" }, { type: "Null" }],
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    ];

    const result = transformRows(rows, relationSchema, "todos", {
      owner: { manager: true },
    });

    expect(result).toEqual([
      {
        id: "todo-1",
        title: "Buy milk",
        owner_id: "user-1",
        owner: {
          name: "Alice",
          manager_id: "user-2",
          manager: {
            name: "Manager",
            manager_id: undefined,
          },
        },
      },
    ]);
  });
});
