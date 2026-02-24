import { describe, it, expect, beforeEach } from "vitest";
import { table, col, resetCollectedState, getCollectedSchema } from "../dsl.js";
import { schemaToWasm } from "./schema-reader.js";
import { generateTypes } from "./type-generator.js";
import { generateClient, analyzeRelations } from "./index.js";
import type { WasmSchema } from "../drivers/types.js";

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

  it("converts TEXT[] to Array<Text>", () => {
    table("items", { tags: col.array(col.string()) });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "tags",
      column_type: { type: "Array", element: { type: "Text" } },
      nullable: false,
    });
  });

  it("converts nested arrays (INTEGER[][])", () => {
    table("items", { matrix: col.array(col.array(col.int())) });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "matrix",
      column_type: {
        type: "Array",
        element: { type: "Array", element: { type: "Integer" } },
      },
      nullable: false,
    });
  });

  it("preserves references for UUID[] from array(ref)", () => {
    table("items", { owner_ids: col.array(col.ref("users")) });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.items.columns[0]).toEqual({
      name: "owner_ids",
      column_type: { type: "Array", element: { type: "Uuid" } },
      nullable: false,
      references: "users",
    });
  });

  it("converts enum to Enum with normalized variants", () => {
    table("tasks", { status: col.enum("in_progress", "todo", "done") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);

    expect(wasm.tables.tasks.columns[0]).toEqual({
      name: "status",
      column_type: { type: "Enum", variants: ["done", "in_progress", "todo"] },
      nullable: false,
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

  it("carries table permissions into wasm schema", () => {
    table("todos", { owner_id: col.string(), title: col.string() });
    const schema = getCollectedSchema();
    const ownerMatchesSession: import("../schema.js").PolicyExpr = {
      type: "Cmp",
      column: "owner_id",
      op: "Eq",
      value: { type: "SessionRef", path: ["user_id"] },
    };

    schema.tables[0]!.policies = {
      select: { using: ownerMatchesSession },
      insert: { with_check: ownerMatchesSession },
      update: { using: ownerMatchesSession, with_check: ownerMatchesSession },
      delete: { using: ownerMatchesSession },
    };

    const wasm = schemaToWasm(schema);

    expect(wasm.tables.todos.policies).toEqual({
      select: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
      update: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
        with_check: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
      delete: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
    });
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

  it("singularises plural table names", () => {
    table("categories", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface Category {");
    expect(output).toContain("export interface CategoryInit {");
  });

  it.each([
    ["canvases", "Canvas"],
    ["statuses", "Status"],
    ["buses", "Bus"],
    ["processes", "Process"],
    ["heroes", "Hero"],
    ["vertices", "Vertex"],
    ["people", "Person"],
    ["matrices", "Matrix"],
    ["addresses", "Address"],
  ])("singularises %s to %s", (tableName, expected) => {
    table(tableName, { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain(`export interface ${expected} {`);
    expect(output).toContain(`export interface ${expected}Init {`);
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
    table("users", { name: col.string() });
    table("todos", { owner_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("  owner_id: string;");
  });

  it("maps array columns recursively", () => {
    table("items", {
      tags: col.array(col.string()),
      matrix: col.array(col.array(col.int())),
    });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("  tags: string[];");
    expect(output).toContain("  matrix: number[][];");
  });

  it("maps enum columns to string literal unions", () => {
    table("tasks", { status: col.enum("in_progress", "todo", "done") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain('  status: "done" | "in_progress" | "todo";');
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

  it("imports WasmSchema and QueryBuilder from jazz-tools", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain('import type { WasmSchema, QueryBuilder } from "jazz-tools";');
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
    expect(output).toContain('import type { WasmSchema, QueryBuilder } from "jazz-tools";');

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

describe("analyzeRelations", () => {
  it("derives forward relations from references", () => {
    const schema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: { columns: [] },
      },
    };

    const relations = analyzeRelations(schema);
    const todoRels = relations.get("todos")!;

    expect(todoRels).toContainEqual(
      expect.objectContaining({
        name: "owner",
        type: "forward",
        toTable: "users",
        isArray: false,
        nullable: false,
      }),
    );
  });

  it("derives reverse relations", () => {
    const schema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: { columns: [] },
      },
    };

    const relations = analyzeRelations(schema);
    const userRels = relations.get("users")!;

    expect(userRels).toContainEqual(
      expect.objectContaining({
        name: "todosViaOwner",
        type: "reverse",
        toTable: "todos",
        isArray: true,
      }),
    );
  });

  it("marks forward UUID[] references as array relations", () => {
    const schema: WasmSchema = {
      tables: {
        files: {
          columns: [
            {
              name: "parts",
              column_type: { type: "Array", element: { type: "Uuid" } },
              nullable: false,
              references: "file_parts",
            },
          ],
        },
        file_parts: { columns: [] },
      },
    };

    const relations = analyzeRelations(schema);
    const fileRels = relations.get("files")!;

    expect(fileRels).toContainEqual(
      expect.objectContaining({
        name: "parts",
        type: "forward",
        toTable: "file_parts",
        isArray: true,
      }),
    );
  });

  it("handles self-referential relations", () => {
    const schema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            {
              name: "parent_id",
              column_type: { type: "Uuid" },
              nullable: true,
              references: "todos",
            },
          ],
        },
      },
    };

    const relations = analyzeRelations(schema);
    const todoRels = relations.get("todos")!;

    // Forward: parent
    expect(todoRels).toContainEqual(
      expect.objectContaining({ name: "parent", type: "forward", nullable: true }),
    );
    // Reverse: todosViaParent
    expect(todoRels).toContainEqual(
      expect.objectContaining({ name: "todosViaParent", type: "reverse", isArray: true }),
    );
  });

  it("handles multiple relations on same table", () => {
    const schema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
            {
              name: "assignee_id",
              column_type: { type: "Uuid" },
              nullable: true,
              references: "users",
            },
          ],
        },
        users: { columns: [] },
      },
    };

    const relations = analyzeRelations(schema);
    const todoRels = relations.get("todos")!;
    const userRels = relations.get("users")!;

    // Forward relations on todos
    expect(todoRels).toContainEqual(expect.objectContaining({ name: "owner" }));
    expect(todoRels).toContainEqual(expect.objectContaining({ name: "assignee" }));

    // Reverse relations on users
    expect(userRels).toContainEqual(expect.objectContaining({ name: "todosViaOwner" }));
    expect(userRels).toContainEqual(expect.objectContaining({ name: "todosViaAssignee" }));
  });

  it("throws error when referencing unknown table", () => {
    const schema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        // Note: "users" table is NOT defined
      },
    };

    expect(() => analyzeRelations(schema)).toThrow(
      'Table "todos" references unknown table "users" via column "owner_id"',
    );
  });

  it("throws for non-UUID references", () => {
    const schema: WasmSchema = {
      tables: {
        files: {
          columns: [
            {
              name: "parts",
              column_type: { type: "Array", element: { type: "Text" } },
              nullable: false,
              references: "file_parts",
            },
          ],
        },
        file_parts: { columns: [] },
      },
    };

    expect(() => analyzeRelations(schema)).toThrow(
      'Column "files.parts" uses references but is not UUID or UUID[]',
    );
  });
});

describe("generateTypes with relations", () => {
  beforeEach(() => {
    resetCollectedState();
  });

  it("generates Include types", () => {
    table("todos", { owner_id: col.ref("users") });
    table("users", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface TodoInclude {");
    // Include types now include QueryBuilder union
    expect(output).toContain("owner?: boolean | UserInclude | UserQueryBuilder;");
  });

  it("generates Relations types", () => {
    table("todos", { owner_id: col.ref("users") });
    table("users", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface TodoRelations {");
    expect(output).toContain("owner: User;");
  });

  it("generates reverse relations as arrays", () => {
    table("todos", { owner_id: col.ref("users") });
    table("users", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface UserRelations {");
    expect(output).toContain("todosViaOwner: Todo[];");
  });

  it("generates WithIncludes types", () => {
    table("todos", { owner_id: col.ref("users") });
    table("users", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export type TodoWithIncludes<I extends TodoInclude = {}>");
    expect(output).toContain("export type UserWithIncludes<I extends UserInclude = {}>");
  });

  it("generates Include types for self-referential tables", () => {
    table("todos", {
      title: col.string(),
      parent_id: col.ref("todos").optional(),
    });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface TodoInclude {");
    // Include types now include QueryBuilder union
    expect(output).toContain("parent?: boolean | TodoInclude | TodoQueryBuilder;");
    expect(output).toContain("todosViaParent?: boolean | TodoInclude | TodoQueryBuilder;");
  });

  it("does not generate relation types for tables without relations", () => {
    table("items", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    // Should still have base and init types
    expect(output).toContain("export interface Item {");
    expect(output).toContain("export interface ItemInit {");

    // Should NOT have Include/Relations/WithIncludes since no relations
    expect(output).not.toContain("export interface ItemInclude {");
    expect(output).not.toContain("export interface ItemRelations {");
    expect(output).not.toContain("export type ItemWithIncludes");
  });
});

describe("generateWhereInputTypes", () => {
  beforeEach(() => {
    resetCollectedState();
  });

  it("generates WhereInput types for basic columns", () => {
    table("todos", { title: col.string(), done: col.boolean(), priority: col.int() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export interface TodoWhereInput {");
    expect(output).toContain("title?: string | { eq?: string; ne?: string; contains?: string };");
    expect(output).toContain("done?: boolean;");
    expect(output).toContain(
      "priority?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };",
    );
  });

  it("generates id filter with in operator", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("id?: string | { eq?: string; ne?: string; in?: string[] };");
  });

  it("generates FK filter with isNull for nullable refs", () => {
    table("users", { name: col.string() });
    table("todos", { owner_id: col.ref("users").optional() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("owner_id?: string | { eq?: string; ne?: string; isNull?: boolean };");
  });

  it("generates FK filter without isNull for required refs", () => {
    table("users", { name: col.string() });
    table("todos", { owner_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("owner_id?: string | { eq?: string; ne?: string };");
  });

  it("generates array filters with eq and contains", () => {
    table("todos", { tags: col.array(col.string()) });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("tags?: string[] | { eq?: string[]; contains?: string };");
  });

  it("generates enum filters with eq/ne/in", () => {
    table("tasks", { status: col.enum("in_progress", "todo", "done") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain(
      'status?: "done" | "in_progress" | "todo" | { eq?: "done" | "in_progress" | "todo"; ne?: "done" | "in_progress" | "todo"; in?: ("done" | "in_progress" | "todo")[] };',
    );
  });
});

describe("generateQueryBuilderClasses", () => {
  beforeEach(() => {
    resetCollectedState();
  });

  it("generates QueryBuilder classes", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export class TodoQueryBuilder<I extends Record<string, never> = {}>");
    expect(output).toContain("declare readonly _rowType: Todo;");
    expect(output).toContain("declare readonly _initType: TodoInit;");
    expect(output).toContain("where(conditions: TodoWhereInput)");
    expect(output).toContain("orderBy(column: keyof Todo");
    expect(output).toContain("limit(n: number)");
    expect(output).toContain("offset(n: number)");
    expect(output).toContain("gather(options: {");
    expect(output).toContain("_build(): string");
  });

  it("generates QueryBuilder with Include constraint for tables with relations", () => {
    table("users", { name: col.string() });
    table("todos", { owner_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export class TodoQueryBuilder<I extends TodoInclude = {}>");
  });

  it("generates include method for tables with relations", () => {
    table("users", { name: col.string() });
    table("todos", { owner_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("include<NewI extends TodoInclude>(relations: NewI)");
  });

  it("generates hopTo method for tables with relations", () => {
    table("users", { name: col.string() });
    table("todos", { owner_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain('hopTo(relation: "owner")');
  });

  it("does not generate include method for tables without relations", () => {
    table("items", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    // ItemQueryBuilder should exist
    expect(output).toContain("export class ItemQueryBuilder");
    // But should not have include method - look for the specific signature
    const itemQueryBuilderMatch = output.match(/export class ItemQueryBuilder[\s\S]*?^}/m);
    expect(itemQueryBuilderMatch).toBeTruthy();
    expect(itemQueryBuilderMatch![0]).not.toContain("include<NewI extends");
  });

  it("updates Include types with QueryBuilder union", () => {
    table("users", { name: col.string() });
    table("todos", { owner_id: col.ref("users") });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("owner?: boolean | UserInclude | UserQueryBuilder;");
  });

  it("QueryBuilder._build() returns valid JSON structure", () => {
    table("todos", { title: col.string(), done: col.boolean() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    // Verify _build method structure exists
    expect(output).toContain("_build(): string {");
    expect(output).toContain("return JSON.stringify({");
    expect(output).toContain("table: this._table,");
    expect(output).toContain("conditions: this._conditions,");
    expect(output).toContain("includes: this._includes,");
    expect(output).toContain("orderBy: this._orderBys,");
    expect(output).toContain("limit: this._limitVal,");
    expect(output).toContain("offset: this._offsetVal,");
    expect(output).toContain("hops: this._hops,");
    expect(output).toContain("gather: this._gatherVal,");
  });

  it("generates private _clone method for immutability", () => {
    table("todos", { title: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("private _clone(): TodoQueryBuilder<I> {");
    expect(output).toContain("const clone = new TodoQueryBuilder<I>();");
    expect(output).toContain("clone._conditions = [...this._conditions];");
    expect(output).toContain("clone._hops = [...this._hops];");
    expect(output).toContain("clone._gatherVal = this._gatherVal");
  });

  it("generates gather helper that compiles start + step", () => {
    table("todos", { title: col.string(), parent_id: col.ref("todos").optional() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("gather(options: {");
    expect(output).toContain("const stepOutput = options.step({ current: currentToken });");
    expect(output).toContain("if (stepHops.length !== 1) {");
    expect(output).toContain("const withStart = this.where(options.start);");
    expect(output).toContain("clone._gatherVal = {");
  });
});

describe("generateAppExport", () => {
  beforeEach(() => {
    resetCollectedState();
  });

  it("generates app export with table proxies", () => {
    table("todos", { title: col.string() });
    table("users", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("export const app = {");
    expect(output).toContain("todos: new TodoQueryBuilder(),");
    expect(output).toContain("users: new UserQueryBuilder(),");
    expect(output).toContain("wasmSchema,");
  });

  it("app export includes wasmSchema reference", () => {
    table("items", { name: col.string() });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    // Verify wasmSchema is defined before app and included in app
    const wasmSchemaIndex = output.indexOf("export const wasmSchema");
    const appIndex = output.indexOf("export const app = {");
    expect(wasmSchemaIndex).toBeLessThan(appIndex);
    expect(output).toContain("wasmSchema,");
  });
});

describe("QueryBuilder self-referential relations", () => {
  beforeEach(() => {
    resetCollectedState();
  });

  it("generates Include with QueryBuilder for self-referential tables", () => {
    table("todos", {
      title: col.string(),
      parent_id: col.ref("todos").optional(),
    });
    const schema = getCollectedSchema();
    const wasm = schemaToWasm(schema);
    const output = generateTypes(wasm);

    expect(output).toContain("parent?: boolean | TodoInclude | TodoQueryBuilder;");
    expect(output).toContain("todosViaParent?: boolean | TodoInclude | TodoQueryBuilder;");
  });
});
