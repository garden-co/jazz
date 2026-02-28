import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder, TableProxy } from "../runtime/db.js";
import { createJazzClient, type JazzClient } from "./create-jazz-client.js";

type Todo = {
  id: string;
  title: string;
  done: boolean;
};

type TodoInsert = {
  title: string;
  done: boolean;
};

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const todosTable: TableProxy<Todo, TodoInsert> = {
  _table: "todos",
  _schema: schema,
  _rowType: undefined as unknown as Todo,
  _initType: undefined as unknown as TodoInsert,
};

const allTodosQuery: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: schema,
  _rowType: undefined as unknown as Todo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

function makeAppId(scope: string): string {
  return `react-create-jazz-client-${scope}-${randomUUID()}`;
}

describe("react/create-jazz-client integration", () => {
  it("RC-I01: supports mutation + query flow via returned db", async () => {
    let client: JazzClient | null = null;

    try {
      client = await createJazzClient({ appId: makeAppId("mutation-query") });

      const id = client.db.insert(todosTable, { title: "buy milk", done: false });
      const rows = await client.db.all(allTodosQuery);

      expect(
        rows.some((row) => row.id === id && row.title === "buy milk" && row.done === false),
      ).toBe(true);
    } finally {
      if (client) {
        await client.shutdown();
      }
    }
  }, 15000);

  it("RC-I02: local auth defaults return a non-null local session in local mode", async () => {
    let client: JazzClient | null = null;

    try {
      client = await createJazzClient({
        appId: makeAppId("local-session"),
        localAuthMode: "anonymous",
      });

      expect(client.session).not.toBeNull();
      expect(client.session?.claims.auth_mode).toBe("local");
      expect(client.session?.claims.local_mode).toBe("anonymous");
    } finally {
      if (client) {
        await client.shutdown();
      }
    }
  }, 15000);

  it("RC-I03: shutdown after activity releases resources cleanly", async () => {
    let client: JazzClient | null = null;

    try {
      client = await createJazzClient({ appId: makeAppId("shutdown") });
      client.db.insert(todosTable, { title: "shutdown-check", done: false });
      await client.db.all(allTodosQuery);

      await expect(client.shutdown()).resolves.toBeUndefined();
      client = null;
    } finally {
      if (client) {
        await client.shutdown();
      }
    }
  }, 15000);
});
