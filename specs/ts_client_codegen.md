# TypeScript Client Codegen Spec

## Overview

Generate a typed TypeScript client from Jazz schemas, providing a high-level query builder API with full type safety for queries, mutations, and subscriptions.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Developer Workflow                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   schema/current.ts ──► jazz-ts build ──► schema/app.ts (generated) │
│                              │                                       │
│                              ▼                                       │
│                     WasmSchema JSON (intermediate)                   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                        Runtime Architecture                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   Application Code                                                   │
│         │                                                            │
│         ▼                                                            │
│   ┌─────────────────┐     ┌─────────────────┐                       │
│   │  Generated      │────▶│  createClient() │                       │
│   │  schema/app.ts  │     │  (generic DB)   │                       │
│   └─────────────────┘     └────────┬────────┘                       │
│                                    │                                 │
│                                    ▼                                 │
│                           ┌─────────────────┐                       │
│                           │   JazzClient    │                       │
│                           │  (groove-wasm)  │                       │
│                           └────────┬────────┘                       │
│                                    │                                 │
│                    ┌───────────────┼───────────────┐                │
│                    ▼               ▼               ▼                │
│              IndexedDB        SQLite Node     Server Sync           │
│              (Browser)        (Node.js)       (SSE/REST)            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Design Decisions

| Decision           | Choice                 | Rationale                                                 |
| ------------------ | ---------------------- | --------------------------------------------------------- |
| Schema source      | WasmSchema JSON        | Already has types resolved, consistent with runtime       |
| Relations          | `col.ref('table')`     | All refs are UUIDs, simple syntax                         |
| Relation naming    | Strip `_id` suffix     | `parent_id` → `.include({ parent })`                      |
| Reverse relations  | `tableViaColumn`       | `blockersViaBlocking` - auto-derived                      |
| Output             | Single `schema/app.ts` | Simple imports, easy to understand                        |
| Subscription shape | Full state + delta     | `{ all, added, updated, removed }`                        |
| DB interface       | Generic + schema       | `createClient(schema)`, `db.all(schema.todos.where(...))` |
| Client contexts    | All                    | Browser (IndexedDB), Node (SQLite), offline-first         |

---

## Part 1: Schema DSL Extension

### 1.1 New `col.ref()` Method

Extend the TypeScript DSL to support foreign key references:

```typescript
// schema/current.ts
import { table, col } from "jazz-ts";

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
  parent_id: col.ref("todos").optional(), // self-reference
  owner_id: col.ref("users"), // required FK
});

table("blockers", {
  blocking_id: col.ref("todos"), // the todo being blocked
  blocked_by_id: col.ref("todos"), // the todo that blocks it
});

table("users", {
  name: col.string(),
  email: col.string(),
});
```

### 1.2 SQL Generation

The `col.ref('table')` generates:

- Column type: `UUID REFERENCES table NOT NULL` (or without NOT NULL if `.optional()`)
- Schema metadata includes `references: "table"` (used by codegen)

```sql
CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    description TEXT,
    parent_id UUID REFERENCES todos,
    owner_id UUID REFERENCES users NOT NULL
);
```

### 1.3 WasmSchema Enhancement

The `WasmColumnDescriptor.references` field is already present in Rust. Ensure the TS DSL populates it:

```typescript
// In WasmSchema JSON output
{
  "tables": {
    "todos": {
      "columns": [
        { "name": "title", "column_type": { "type": "Text" }, "nullable": false },
        { "name": "parent_id", "column_type": { "type": "Uuid" }, "nullable": true, "references": "todos" },
        { "name": "owner_id", "column_type": { "type": "Uuid" }, "nullable": false, "references": "users" }
      ]
    }
  }
}
```

---

## Part 2: Generated Client API

### 2.1 Usage Example

```typescript
import { createDb } from "jazz-ts";
import { app } from "./schema/app.js";

// Create db builder (works in browser or Node.js)
// The db builder lazily creates and memoizes JazzClient instances per schema
const db = createDb({
  appId: "my-app",
  driver: indexedDbDriver(), // or sqliteDriver(), etc.
  serverUrl: "http://localhost:1625", // optional sync
});

// Build typed queries
const query = app.todos
  .where({ done: false, priority: { gt: 2 } })
  .include({
    parent: true, // FK: parent_id → todos
    owner: true, // FK: owner_id → users
    blockersViaBlocking: {
      // Reverse: blockers.blocking_id
      blockedBy: true, // FK: blocked_by_id → todos
    },
  })
  .orderBy("priority", "desc")
  .limit(10);

// One-shot queries - db uses query's schema to get/create the right client
const todos = await db.all(query); // Todo[] with included relations
const todo = await db.one(query); // Todo | null

// Subscriptions with full state + delta
const unsub = db.subscribeAll(query, ({ all, added, updated, removed }) => {
  console.log("All todos:", all);
  console.log("New:", added);
  console.log("Changed:", updated);
  console.log("Deleted:", removed);
});

// Typed mutations - table proxy carries schema reference
const id = await db.insert(app.todos, {
  title: "New task",
  done: false,
  priority: 3,
  owner_id: currentUserId,
});

await db.update(id, { done: true });
await db.delete(id);
```

### 2.1.1 Db Builder Architecture

The `createDb()` returns a builder that:

1. Does NOT require schema at initialization
2. Extracts schema from query builders / table proxies at call time
3. Memoizes `JazzClient` instances per schema (by schema hash)
4. Allows multiple schemas to coexist in one app

```typescript
// Internal: db builder memoizes clients
class DbBuilder {
  private clients = new Map<string, JazzClient>();

  async getClient(schema: WasmSchema): Promise<JazzClient> {
    const hash = schemaHash(schema);
    if (!this.clients.has(hash)) {
      const client = await JazzClient.connect({
        ...this.config,
        schema,
      });
      this.clients.set(hash, client);
    }
    return this.clients.get(hash)!;
  }

  async all<T>(query: QueryBuilder<T>): Promise<T[]> {
    const client = await this.getClient(query._schema);
    return client.query(query._build());
  }
}
```

### 2.2 Generated `schema/app.ts` Structure

```typescript
// AUTO-GENERATED FILE - DO NOT EDIT
// Generated from schema version: abc123...

import type { WasmSchema, Value } from "jazz-ts";

// ============================================================================
// Base Types
// ============================================================================

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  parent_id?: string;
  owner_id: string;
}

export interface Blocker {
  id: string;
  blocking_id: string;
  blocked_by_id: string;
}

export interface User {
  id: string;
  name: string;
  email: string;
}

// ============================================================================
// Init Types (for insert/update)
// ============================================================================

export interface TodoInit {
  title: string;
  done: boolean;
  description?: string;
  parent_id?: string;
  owner_id: string;
}

export interface BlockerInit {
  blocking_id: string;
  blocked_by_id: string;
}

export interface UserInit {
  name: string;
  email: string;
}

// ============================================================================
// Include Types (for specifying relation depth)
// ============================================================================

export interface TodoInclude {
  parent?: boolean | TodoInclude | TodoQueryBuilder;
  owner?: boolean | UserInclude | UserQueryBuilder;
  blockersViaBlocking?: boolean | BlockerInclude | BlockerQueryBuilder;
}

export interface BlockerInclude {
  blocking?: boolean | TodoInclude | TodoQueryBuilder;
  blockedBy?: boolean | TodoInclude | TodoQueryBuilder;
}

export interface UserInclude {
  todosViaOwner?: boolean | TodoInclude | TodoQueryBuilder;
}

// ============================================================================
// Generic WithIncludes Types (type-safe results based on include depth)
// ============================================================================

// Resolve include specification to actual type
type ResolveInclude<T, I> = I extends true
  ? T
  : I extends { [K: string]: any }
    ? T & ResolveIncludes<T, I>
    : never;

type ResolveIncludes<T, I> = {
  [K in keyof I]: I[K] extends boolean | object
    ? K extends keyof TodoRelations
      ? ResolveInclude<TodoRelations[K], I[K]>
      : never
    : never;
};

// Relation mappings for each type
interface TodoRelations {
  parent: Todo;
  owner: User;
  blockersViaBlocking: Blocker[];
}

interface BlockerRelations {
  blocking: Todo;
  blockedBy: Todo;
}

interface UserRelations {
  todosViaOwner: Todo[];
}

// Generic result type that infers included relations
export type TodoWithIncludes<I extends TodoInclude> = Todo & {
  [K in keyof I & keyof TodoRelations]?: I[K] extends true
    ? TodoRelations[K]
    : I[K] extends object
      ? TodoRelations[K] extends any[]
        ? Array<TodoRelations[K][number] & ResolveIncludes<TodoRelations[K][number], I[K]>>
        : TodoRelations[K] & ResolveIncludes<TodoRelations[K], I[K]>
      : never;
};

export type BlockerWithIncludes<I extends BlockerInclude> = Blocker & {
  [K in keyof I & keyof BlockerRelations]?: I[K] extends true
    ? BlockerRelations[K]
    : I[K] extends object
      ? BlockerRelations[K] & ResolveIncludes<BlockerRelations[K], I[K]>
      : never;
};

export type UserWithIncludes<I extends UserInclude> = User & {
  [K in keyof I & keyof UserRelations]?: I[K] extends true
    ? UserRelations[K]
    : I[K] extends object
      ? UserRelations[K] extends any[]
        ? Array<UserRelations[K][number] & ResolveIncludes<UserRelations[K][number], I[K]>>
        : UserRelations[K] & ResolveIncludes<UserRelations[K], I[K]>
      : never;
};

// ============================================================================
// Query Builders
// ============================================================================

export interface TodoWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  title?: string | { eq?: string; ne?: string; contains?: string };
  done?: boolean;
  priority?: number | { eq?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  parent_id?: string | { eq?: string; isNull?: boolean };
  owner_id?: string;
}

export class TodoQueryBuilder<I extends TodoInclude = {}> {
  where(conditions: TodoWhereInput): TodoQueryBuilder<I>;
  include<NewI extends TodoInclude>(relations: NewI): TodoQueryBuilder<I & NewI>;
  orderBy(column: keyof Todo, direction?: "asc" | "desc"): TodoQueryBuilder<I>;
  limit(n: number): TodoQueryBuilder<I>;
  offset(n: number): TodoQueryBuilder<I>;

  // Type helper for db.all() result inference
  _resultType: TodoWithIncludes<I>;

  // Internal
  _schema: WasmSchema;
  _build(): string;
}

// ============================================================================
// Schema Export
// ============================================================================

export const app = {
  todos: new TodoQueryBuilder("todos"),
  blockers: new BlockerQueryBuilder("blockers"),
  users: new UserQueryBuilder("users"),

  // Raw WasmSchema for createClient()
  wasmSchema: {
    /* ... */
  } as WasmSchema,
};
```

### 2.3 Generic DB Interface

The `createDb()` function returns a builder that lazily creates clients:

```typescript
// packages/jazz-ts/src/runtime/db.ts

export interface DbConfig {
  appId: string;
  driver: StorageDriver;
  serverUrl?: string;
  env?: string;
  userBranch?: string;
}

export interface Db {
  // Queries - result type inferred from query builder's include spec
  all<T, I>(query: QueryBuilder<T, I>): Promise<Array<T & ResolvedIncludes<T, I>>>;
  one<T, I>(query: QueryBuilder<T, I>): Promise<(T & ResolvedIncludes<T, I>) | null>;

  // Subscriptions
  subscribeAll<T, I>(
    query: QueryBuilder<T, I>,
    callback: (delta: {
      all: Array<T & ResolvedIncludes<T, I>>;
      added: Array<T & ResolvedIncludes<T, I>>;
      updated: Array<T & ResolvedIncludes<T, I>>;
      removed: Array<T & ResolvedIncludes<T, I>>;
    }) => void,
  ): () => void;

  // Mutations - use Init types, not full entity
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<string>;
  update<Init>(id: string, data: Partial<Init>): Promise<void>;
  delete(id: string): Promise<void>;

  // Lifecycle
  shutdown(): Promise<void>;
}

export function createDb(config: DbConfig): Db {
  return new DbBuilder(config);
}
```

---

## Part 3: Codegen Implementation

### 3.1 CLI Integration

Extend `jazz-ts build` to generate the client:

```bash
# Existing: compiles TS DSL to SQL, calls jazz build
jazz-ts build --schema-dir ./schema

# Output:
# - schema/schema_v1_abc123.sql (existing)
# - schema/app.ts (NEW - generated client)
```

### 3.2 Codegen Pipeline

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  current.ts     │────▶│   WasmSchema    │────▶│    app.ts       │
│  (TS DSL)       │     │   (JSON)        │     │  (generated)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
   col.ref('x')          references: "x"        include({ x })
```

### 3.3 Codegen Module Structure

```
packages/jazz-ts/src/
├── codegen/
│   ├── index.ts           # Main codegen entry point
│   ├── schema-reader.ts   # Parse WasmSchema JSON
│   ├── relation-analyzer.ts # Derive forward + reverse relations
│   ├── type-generator.ts  # Generate TypeScript interfaces
│   ├── query-builder-generator.ts # Generate query builders
│   └── templates/         # Code templates
│       ├── header.ts.template
│       ├── types.ts.template
│       └── query-builder.ts.template
└── cli.ts                 # Add codegen step after build
```

### 3.4 Relation Analysis Algorithm

```typescript
interface Relation {
  name: string; // e.g., "parent" or "blockersViaBlocking"
  type: "forward" | "reverse";
  fromTable: string;
  toTable: string;
  fromColumn: string; // FK column name
  toColumn: string; // Always "id" for reverse
  isArray: boolean; // true for reverse relations
}

function analyzeRelations(schema: WasmSchema): Map<string, Relation[]> {
  const relations = new Map<string, Relation[]>();

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const tableRelations: Relation[] = [];

    for (const col of table.columns) {
      if (col.references) {
        // Forward relation: parent_id → parent
        const name = col.name.replace(/_id$/, "");
        tableRelations.push({
          name,
          type: "forward",
          fromTable: tableName,
          toTable: col.references,
          fromColumn: col.name,
          toColumn: "id",
          isArray: false,
        });

        // Reverse relation on target table: todosViaParent
        const reverseName = `${tableName}Via${capitalize(name)}`;
        addReverse(relations, col.references, {
          name: reverseName,
          type: "reverse",
          fromTable: col.references,
          toTable: tableName,
          fromColumn: "id",
          toColumn: col.name,
          isArray: true,
        });
      }
    }

    relations.set(tableName, tableRelations);
  }

  return relations;
}
```

---

## Part 4: Query Execution

### 4.1 Query Builder to WasmQueryBuilder Translation

The generated query builders translate to `WasmQueryBuilder` calls:

```typescript
// User writes:
app.todos
  .where({ done: false, priority: { gt: 2 } })
  .include({ parent: true })
  .orderBy("priority", "desc")
  .limit(10);

// Translates to:
new WasmQueryBuilder("todos")
  .filterEq("done", { type: "Boolean", value: false })
  .filterGt("priority", { type: "Integer", value: 2 })
  .with_array("parent", (sub) => sub.from("todos").correlate("id", "todos.parent_id"))
  .orderByDesc("priority")
  .limit(10)
  .build();
```

### 4.2 Result Transformation

Transform `WasmRow[]` to typed objects:

```typescript
function rowToTodo(row: WasmRow, schema: WasmTableSchema): Todo {
  const result: Record<string, unknown> = { id: row.id };

  for (let i = 0; i < schema.columns.length; i++) {
    const col = schema.columns[i];
    const value = row.values[i];
    result[col.name] = unwrapValue(value);
  }

  return result as Todo;
}

function unwrapValue(v: WasmValue): unknown {
  switch (v.type) {
    case "Text":
      return v.value;
    case "Boolean":
      return v.value;
    case "Integer":
      return v.value;
    case "Null":
      return undefined;
    case "Array":
      return v.value.map(unwrapValue);
    case "Row":
      return v.value.map(unwrapValue);
    // etc.
  }
}
```

### 4.3 Subscription State Management

Maintain full result set, compute delta, and preserve object identity for unchanged items:

```typescript
class SubscriptionManager<T extends { id: string }> {
  private currentResults: Map<string, T> = new Map();

  handleDelta(delta: WasmRowDelta, transform: (row: WasmRow) => T): SubscriptionDelta<T> {
    const added: T[] = [];
    const updated: T[] = [];
    const removed: T[] = [];

    // Process additions - new objects
    for (const row of delta.added) {
      const item = transform(row);
      this.currentResults.set(row.id, item);
      added.push(item);
    }

    // Process updates - replace only changed objects, keep identity for unchanged
    for (const [oldRow, newRow] of delta.updated) {
      const newItem = transform(newRow);
      const existingItem = this.currentResults.get(newRow.id);

      // Only replace if actually changed (shallow compare or hash)
      // This preserves object identity for React's referential equality checks
      if (!existingItem || !shallowEqual(existingItem, newItem)) {
        this.currentResults.set(newRow.id, newItem);
        updated.push(newItem);
      }
    }

    // Process removals
    for (const row of delta.removed) {
      const item = this.currentResults.get(row.id);
      if (item) {
        this.currentResults.delete(row.id);
        removed.push(item);
      }
    }

    // Return stable array reference if nothing changed
    // (optimization for React's useMemo/useEffect dependencies)
    return {
      all: Array.from(this.currentResults.values()),
      added,
      updated,
      removed,
    };
  }
}

function shallowEqual<T extends object>(a: T, b: T): boolean {
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  for (const key of keysA) {
    if ((a as any)[key] !== (b as any)[key]) return false;
  }
  return true;
}
```

```

---

## Part 5: Example App - example-todo-ts-client

A browser-based vanilla TypeScript app demonstrating the generated client.

### 5.1 Structure

```

examples/todo-ts-client/
├── package.json
├── tsconfig.json
├── index.html
├── src/
│ └── main.ts
└── schema/
├── current.ts # Schema with relations
└── app.ts # Generated client (after build)

````

### 5.2 Schema

```typescript
// examples/todo-ts-client/schema/current.ts
import { table, col } from "jazz-ts";

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
  priority: col.int(),
  parent_id: col.ref("todos").optional(),
});
````

### 5.3 Application Code

```typescript
// examples/todo-ts-client/src/main.ts
import { createDb, indexedDbDriver } from "jazz-ts";
import { app } from "../schema/app.js";

async function main() {
  const db = createDb({
    appId: "todo-client-example",
    driver: indexedDbDriver(),
    serverUrl: "http://localhost:1625",
  });

  // Render function
  function render(todos: (typeof app.todos._resultType)[]) {
    const list = document.getElementById("todo-list")!;
    list.innerHTML = todos
      .map(
        (t) => `
      <li class="${t.done ? "done" : ""}">
        <input type="checkbox" ${t.done ? "checked" : ""}
               onchange="toggleTodo('${t.id}')">
        <span>${t.title}</span>
        ${t.parent ? `<small>(child of: ${t.parent.title})</small>` : ""}
        <button onclick="deleteTodo('${t.id}')" class="delete-btn">×</button>
      </li>
    `,
      )
      .join("");
  }

  // Subscribe to todos with parent relation
  const query = app.todos
    .where({ done: false })
    .include({ parent: true })
    .orderBy("priority", "desc");

  db.subscribeAll(query, ({ all }) => render(all));

  // Add todo form
  document.getElementById("add-form")!.onsubmit = async (e) => {
    e.preventDefault();
    const input = document.getElementById("title-input") as HTMLInputElement;
    await db.insert(app.todos, {
      title: input.value,
      done: false,
      priority: 1,
    });
    input.value = "";
  };

  // Toggle handler
  (window as any).toggleTodo = async (id: string) => {
    const todo = await db.one(app.todos.where({ id }));
    if (todo) {
      await db.update(id, { done: !todo.done });
    }
  };

  // Delete handler
  (window as any).deleteTodo = async (id: string) => {
    await db.delete(id);
  };
}

main();
```

---

## Part 6: Testing Strategy

### 6.1 Unit Tests (Mock Schemas)

```typescript
// packages/jazz-ts/tests/codegen/codegen.test.ts

describe("Codegen", () => {
  it("generates types for simple schema", async () => {
    const schema: WasmSchema = {
      tables: {
        users: {
          columns: [
            { name: "name", column_type: { type: "Text" }, nullable: false },
            { name: "email", column_type: { type: "Text" }, nullable: false },
          ],
        },
      },
    };

    const output = generateClient(schema);

    expect(output).toContain("export interface User {");
    expect(output).toContain("name: string;");
    expect(output).toContain("email: string;");
  });

  it("generates forward relations from references", async () => {
    const schema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      },
    };

    const output = generateClient(schema);

    expect(output).toContain("owner?: User;");
    expect(output).toContain("todosViaOwner?: Todo[];");
  });

  it("generates self-referential relations", async () => {
    const schema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
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

    const output = generateClient(schema);

    expect(output).toContain("parent?: Todo;");
    expect(output).toContain("todosViaParent?: Todo[];");
  });
});
```

### 6.2 Integration Tests (Query Builder)

```typescript
// packages/jazz-ts/tests/codegen/query-builder.test.ts

describe("Generated Query Builder", () => {
  let app: GeneratedApp;

  beforeAll(async () => {
    app = await generateAndLoad(testSchema);
  });

  it("builds simple where clause", () => {
    const query = app.todos.where({ done: false });
    const json = JSON.parse(query._build());

    expect(json.table).toBe("todos");
    expect(json.disjuncts[0].conditions).toContainEqual({
      Eq: { column: "done", value: { Boolean: false } },
    });
  });

  it("builds comparison operators", () => {
    const query = app.todos.where({ priority: { gt: 5, lte: 10 } });
    const json = JSON.parse(query._build());

    expect(json.disjuncts[0].conditions).toContainEqual({
      Gt: { column: "priority", value: { Integer: 5 } },
    });
    expect(json.disjuncts[0].conditions).toContainEqual({
      Le: { column: "priority", value: { Integer: 10 } },
    });
  });

  it("builds include with array subquery", () => {
    const query = app.todos.include({ parent: true });
    const json = JSON.parse(query._build());

    expect(json.array_subqueries).toHaveLength(1);
    expect(json.array_subqueries[0].column_name).toBe("parent");
    expect(json.array_subqueries[0].table).toBe("todos");
  });
});
```

### 6.3 E2E Tests (With Server)

```typescript
// examples/todo-ts-client/tests/e2e.test.ts

describe("E2E Client Tests", () => {
  let server: ChildProcess;
  let db: Db;

  beforeAll(async () => {
    // Start jazz-cli server
    server = spawn("jazz", ["server", "test-app", "--port", "1626"]);
    await waitForServer("http://localhost:1626/health");

    // Connect client
    db = createDb({
      appId: "test-app",
      driver: indexedDbDriver(),
      serverUrl: "http://localhost:1626",
    });
  });

  afterAll(async () => {
    await db.shutdown();
    server.kill();
  });

  it("creates and queries todos", async () => {
    const id = await db.insert(app.todos, {
      title: "Test todo",
      done: false,
      priority: 5,
    });

    const todos = await db.all(app.todos.where({ id }));

    expect(todos).toHaveLength(1);
    expect(todos[0].title).toBe("Test todo");
  });

  it("deletes todos", async () => {
    const id = await db.insert(app.todos, {
      title: "To be deleted",
      done: false,
      priority: 1,
    });

    await db.delete(id);

    const todos = await db.all(app.todos.where({ id }));
    expect(todos).toHaveLength(0);
  });

  it("syncs between clients", async () => {
    const db2 = createDb({
      appId: "test-app",
      driver: indexedDbDriver(),
      serverUrl: "http://localhost:1626",
    });

    // Create on db1
    const id = await db.insert(app.todos, {
      title: "Sync test",
      done: false,
      priority: 1,
    });

    // Wait for sync
    await new Promise((r) => setTimeout(r, 1000));

    // Query on db2
    const todos = await db2.all(app.todos.where({ id }));
    expect(todos).toHaveLength(1);
    expect(todos[0].title).toBe("Sync test");

    await db2.shutdown();
  });

  it("receives subscription updates", async () => {
    const updates: any[] = [];

    const unsub = db.subscribeAll(app.todos.where({ done: false }), (delta) => updates.push(delta));

    // Create todo
    await db.insert(app.todos, {
      title: "Sub test",
      done: false,
      priority: 1,
    });

    await new Promise((r) => setTimeout(r, 500));

    expect(updates.length).toBeGreaterThan(0);
    expect(updates[updates.length - 1].added.length).toBeGreaterThan(0);

    unsub();
  });

  it("preserves object identity for unchanged items in subscriptions", async () => {
    let lastAll: any[] = [];
    const unsub = db.subscribeAll(app.todos.orderBy("priority", "desc"), ({ all }) => {
      lastAll = all;
    });

    // Create first todo
    const id1 = await db.insert(app.todos, { title: "First", done: false, priority: 1 });
    await new Promise((r) => setTimeout(r, 200));
    const firstSnapshot = [...lastAll];

    // Create second todo
    await db.insert(app.todos, { title: "Second", done: false, priority: 2 });
    await new Promise((r) => setTimeout(r, 200));

    // The first todo object should be the same reference (identity preserved)
    const todo1InFirst = firstSnapshot.find((t) => t.id === id1);
    const todo1InSecond = lastAll.find((t) => t.id === id1);
    expect(todo1InFirst).toBe(todo1InSecond); // Same object reference

    unsub();
  });
});
```

---

## Implementation Phases

### Phase 1: DSL Extension

**Goal**: Add `col.ref()` to the TypeScript DSL

Files to modify:

- `packages/jazz-ts/src/dsl.ts` - Add RefBuilder class
- `packages/jazz-ts/src/schema.ts` - Add references to Column type
- `packages/jazz-ts/src/sql-gen.ts` - Generate UUID type for refs

Deliverable: `col.ref('table')` works and produces correct SQL + schema metadata

### Phase 2: Codegen Foundation

**Goal**: Generate basic typed interfaces from WasmSchema

Files to create:

- `packages/jazz-ts/src/codegen/index.ts`
- `packages/jazz-ts/src/codegen/schema-reader.ts`
- `packages/jazz-ts/src/codegen/type-generator.ts`

Deliverable: `jazz-ts build` generates `schema/app.ts` with typed interfaces (no query builders yet)

### Phase 3: Relation Analysis

**Goal**: Derive forward and reverse relations from schema

Files to create:

- `packages/jazz-ts/src/codegen/relation-analyzer.ts`

Deliverable: Generated interfaces include relation types (`TodoWithRelations`, etc.)

### Phase 4: Query Builder Generation

**Goal**: Generate typed query builders with where/include/orderBy

Files to create:

- `packages/jazz-ts/src/codegen/query-builder-generator.ts`

Deliverable: `app.todos.where({...}).include({...})` compiles and type-checks

### Phase 5: Runtime Integration

**Goal**: Connect generated query builders to JazzClient

Files to modify:

- `packages/jazz-ts/src/runtime/client.ts` - Add TypedClient wrapper

Deliverable: `db.all(query)`, `db.one(query)`, `db.insert()` work at runtime

### Phase 6: Subscription Support

**Goal**: Implement full-state subscription management

Files to create/modify:

- `packages/jazz-ts/src/runtime/subscription-manager.ts`
- `packages/jazz-ts/src/runtime/client.ts` - Add subscribeAll

Deliverable: `db.subscribeAll(query, callback)` with `{ all, added, updated, removed }`

### Phase 7: Example App

**Goal**: Create example-todo-ts-client demonstrating all features

Files to create:

- `examples/todo-ts-client/` (full example app)

Deliverable: Working browser app with typed queries and live sync

### Phase 8: Test Suite

**Goal**: Comprehensive tests for codegen and runtime

Files to create:

- `packages/jazz-ts/tests/codegen/*.test.ts`
- `examples/todo-ts-client/tests/e2e.test.ts`

Deliverable: CI-ready test suite covering types, query building, and E2E sync

---

## Resolved Questions

1. **Pagination**: Use `offset(n)` and `limit(n)` - simple start/limit approach for now
2. **Optimistic updates**: Already happens automatically due to local-first architecture - in-memory state and queries update immediately, likely with local storage persistence
3. **Schema versioning**: Already works via SchemaManager - see `specs/schema_manager.md` for details on multi-version support

## Future Work

1. **React/Vue bindings**: Generate framework-specific hooks (e.g., `useTodos()`) - deferred
2. **Conflict resolution**: Surface merge conflicts to application layer - not yet
3. **Cursor-based pagination**: For very large result sets with stable ordering - evaluate later
