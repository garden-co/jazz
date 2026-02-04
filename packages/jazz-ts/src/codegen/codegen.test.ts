import { describe, it, expect, beforeEach } from "vitest";
import { table, col, resetCollectedState, getCollectedSchema } from "../dsl.js";
import { schemaToWasm } from "./schema-reader.js";
import { generateTypes } from "./type-generator.js";
import { generateClient } from "./index.js";

describe("schemaToWasm", () => {
  beforeEach(() => {
    resetCollectedState();
  });

  it("converts TEXT to Text", () => {
    table("items", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "name",
      column_type: { type: "Text" },
      nullable: false,
    });
  });

  it("converts BOOLEAN to Boolean", () => {
    table("items", { active: col.boolean() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "active",
      column_type: { type: "Boolean" },
      nullable: false,
    });
  });

  it("converts INTEGER to Integer", () => {
    table("items", { count: col.int() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "count",
      column_type: { type: "Integer" },
      nullable: false,
    });
  });

  it("converts REAL to Integer (no Float in WASM)", () => {
    table("items", { price: col.float() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "price",
      column_type: { type: "Integer" },
      nullable: false,
    });
  });

  it("converts ref to Uuid with references", () => {
    table("items", { owner_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "owner_id",
      column_type: { type: "Uuid" },
      nullable: false,
      references: "users",
    });
  });

  it("handles nullable columns", () => {
    table("items", { description: col.string().optional() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "description",
      column_type: { type: "Text" },
      nullable: true,
    });
  });

  it("handles nullable refs", () => {
    table("todos", { parent_id: col.ref("todos").optional() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.todos.columns[0]).toEqual({
      name: "parent_id",
      column_type: { type: "Uuid" },
      nullable: true,
      references: "todos",
    });
  });

  it("converts multiple tables", () => {
    table("users", { name: col.string() });
    table("todos", { title: col.string(), user_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(Object.keys(wasm.tables)).toEqual(["users", "todos"]);
    expect(wasm.tables.users.columns).toHaveLength(1);
    expect(wasm.tables.todos.columns).toHaveLength(2);
  });
});

describe("generateTypes", () => {
  beforeEach(() => {
    resetCollectedState();
  });

  it("generates base interface with id field", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface Todo {");
    expect(output).toContain("  id: string;");
    expect(output).toContain("  title: string;");
  });

  it("generates init interface without id field", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface TodoInit {");
    // TodoInit should have title but NOT id
    const initMatch = output.match(/export interface TodoInit \{([^}]+)\}/);
    expect(initMatch).toBeTruthy();
    expect(initMatch![1]).toContain("title: string;");
    expect(initMatch![1]).not.toContain("id:");
  });

  it("handles nullable columns with ?", () => {
    table("todos", {
      title: col.string(),
      description: col.string().optional(),
    });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("  title: string;");
    expect(output).toContain("  description?: string;");
  });

  it("converts snake_case to PascalCase", () => {
    table("user_profiles", { display_name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface UserProfile {");
    expect(output).toContain("export interface UserProfileInit {");
  });

  it("removes trailing s for plurals", () => {
    table("categories", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface Category {");
    expect(output).toContain("export interface CategoryInit {");
  });

  it("maps boolean columns to boolean type", () => {
    table("todos", { done: col.boolean() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("  done: boolean;");
  });

  it("maps int columns to number type", () => {
    table("items", { count: col.int() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("  count: number;");
  });

  it("maps ref columns to string type", () => {
    table("todos", { owner_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("  owner_id: string;");
  });

  it("exports wasmSchema constant", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export const wasmSchema: WasmSchema = {");
    expect(output).toContain('"tables"');
    expect(output).toContain('"todos"');
  });

  it("imports WasmSchema from jazz-ts", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain('import type { WasmSchema } from "jazz-ts";');
  });

  it("includes auto-generated header comment", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("// AUTO-GENERATED FILE - DO NOT EDIT");
  });
});

describe("generateClient", () => {
  beforeEach(() => {
    resetCollectedState();
  });

  it("produces complete output for todos example", () => {
    table("todos", {
      title: col.string(),
      done: col.boolean(),
      parent_id: col.ref("todos").optional(),
    });
    const schema = getCollectedSchema();
    const output = generateClient(schema);

    // Header
    expect(output).toContain("// AUTO-GENERATED FILE - DO NOT EDIT");
    expect(output).toContain('import type { WasmSchema } from "jazz-ts";');

    // Base interface
    expect(output).toContain("export interface Todo {");
    expect(output).toContain("  id: string;");
    expect(output).toContain("  title: string;");
    expect(output).toContain("  done: boolean;");
    expect(output).toContain("  parent_id?: string;");

    // Init interface
    expect(output).toContain("export interface TodoInit {");
    const initMatch = output.match(/export interface TodoInit \{([^}]+)\}/);
    expect(initMatch).toBeTruthy();
    expect(initMatch![1]).not.toContain("id:");

    // wasmSchema export
    expect(output).toContain("export const wasmSchema: WasmSchema =");
    expect(output).toContain('"type": "Text"');
    expect(output).toContain('"type": "Boolean"');
    expect(output).toContain('"type": "Uuid"');
    expect(output).toContain('"references": "todos"');
  });
});
