import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { plainSchema, type PlainEntries } from "./plain-schema.js";

const stored = {
  tasks: {
    columns: [
      { name: "title", column_type: { type: "Bytea" }, nullable: false },
      { name: "__e2ee_eq_title", column_type: { type: "Bytea" }, nullable: true },
      { name: "label", column_type: { type: "Text" }, nullable: true },
    ],
  },
} as unknown as WasmSchema;

const declared = (nullable: boolean): PlainEntries => ({
  tasks: {
    columns: { title: { type: { columnType: { type: "Text" }, nullable } } },
    equality: ["title"],
  },
});

const columns = (schema: WasmSchema) =>
  (schema.tasks as unknown as { columns: { name: string; nullable: boolean }[] }).columns.map(
    (column) => [column.name, column.nullable],
  );

describe("plainSchema", () => {
  it("gives encrypted columns their plaintext type and hides the equality tokens", () => {
    const plain = plainSchema(stored, declared(false));
    expect(columns(plain)).toEqual([
      ["title", false],
      ["label", true],
    ]);
    expect(
      (plain.tasks as { columns: { column_type: unknown }[] }).columns[0]!.column_type,
    ).toEqual({ type: "Text" });
  });

  it("answers each set of declarations for the same schema with its own view", () => {
    const required = plainSchema(stored, declared(false));
    const optional = plainSchema(stored, declared(true));
    expect(columns(required)[0]).toEqual(["title", false]);
    expect(columns(optional)[0]).toEqual(["title", true]);
    const again = declared(false);
    expect(plainSchema(stored, again)).toBe(plainSchema(stored, again));
  });

  it("makes encrypted columns optional on a keyless backend, whose reads never carry them", () => {
    const entries = declared(false);
    const keyless = plainSchema(stored, entries, { keyless: true });
    expect(columns(keyless)).toEqual([
      ["title", true],
      ["label", true],
    ]);
    // The client view of the same declarations is unchanged.
    expect(columns(plainSchema(stored, entries))[0]).toEqual(["title", false]);
  });

  it("hides declared equality tokens and keeps ordinary columns with the same prefix", () => {
    const stored: WasmSchema = {
      tasks: {
        columns: [
          { name: "title", column_type: { type: "Bytea" }, nullable: false },
          { name: "privateNote", column_type: { type: "Bytea" }, nullable: false },
          { name: "__e2ee_eq_title", column_type: { type: "Bytea" }, nullable: false },
          { name: "__e2ee_eq_privateNote", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const view = plainSchema(stored, {
      tasks: {
        columns: {
          title: { type: { columnType: { type: "Text" }, nullable: false } },
          privateNote: { type: { columnType: { type: "Text" }, nullable: true } },
        },
        equality: ["title"],
      },
    });

    expect(view.tasks!.columns).toEqual([
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "privateNote", column_type: { type: "Text" }, nullable: true },
      { name: "__e2ee_eq_privateNote", column_type: { type: "Text" }, nullable: false },
    ]);
    expect(plainSchema(stored)).toBe(stored);
    expect(stored.tasks!.columns).toHaveLength(4);
  });
});
