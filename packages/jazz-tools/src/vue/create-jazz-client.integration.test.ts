import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema } from "../index.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { createJazzClient, type JazzClient } from "./create-jazz-client.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});
const todosTable = app.todos;
const allTodosQuery = app.todos;

function makeAppId(scope: string): string {
  return `vue-create-jazz-client-${scope}-${randomUUID()}`;
}

describe("vue/create-jazz-client integration", () => {
  it("VU-I01: supports mutation + query flow via returned db", async () => {
    let client: JazzClient | null = null;

    try {
      client = await createJazzClient(await localAccountConfig(makeAppId("mutation-query")));

      const { value: inserted } = await client.db.insert(todosTable, {
        title: "buy milk",
        done: false,
      });
      const rows = await client.db.all(allTodosQuery, { tier: "local" });

      expect(
        rows.some(
          (row) => row.id === inserted.id && row.title === "buy milk" && row.done === false,
        ),
      ).toBe(true);
    } finally {
      if (client) {
        await client.shutdown();
      }
    }
  }, 15000);

  it("VU-I03: shutdown after activity releases resources cleanly", async () => {
    let client: JazzClient | null = null;

    try {
      client = await createJazzClient(await localAccountConfig(makeAppId("shutdown")));
      await client.db.insert(todosTable, { title: "shutdown-check", done: false });
      await client.db.all(allTodosQuery, { tier: "local" });

      await expect(client.shutdown()).resolves.toBeUndefined();
      client = null;
    } finally {
      if (client) {
        await client.shutdown();
      }
    }
  }, 15000);
});
