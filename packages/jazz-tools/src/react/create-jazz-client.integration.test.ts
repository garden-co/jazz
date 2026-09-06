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
  return `react-create-jazz-client-${scope}-${randomUUID()}`;
}

describe("react/create-jazz-client integration", () => {
  it("RC-I01: supports mutation + query flow via returned db", async () => {
    let client: JazzClient | null = null;

    try {
      client = await createJazzClient(await localAccountConfig(makeAppId("mutation-query")));
      await client.db.disconnect();

      const { value: inserted } = await client.db.insert(todosTable, {
        title: "buy milk",
        done: false,
      });
      const rows = await client.db.all(allTodosQuery);

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

  it("RC-I02: uses a caller-supplied uuid for inserts", async () => {
    let client: JazzClient | null = null;
    const externalId = "550e8400-e29b-41d4-a716-446655440000";

    try {
      client = await createJazzClient(await localAccountConfig(makeAppId("external-id")));
      await client.db.disconnect();

      const { value: inserted } = await client.db.insert(
        todosTable,
        { title: "with external id", done: false },
        { id: externalId },
      );
      const rows = await client.db.all(allTodosQuery);

      expect(inserted.id).toBe(externalId);
      expect(
        rows.some(
          (row) => row.id === externalId && row.title === "with external id" && row.done === false,
        ),
      ).toBe(true);
    } finally {
      if (client) {
        await client.shutdown();
      }
    }
  }, 15000);

  it("RC-I03: shutdown after activity releases resources cleanly", async () => {
    let client: JazzClient | null = null;

    try {
      client = await createJazzClient(await localAccountConfig(makeAppId("shutdown")));
      await client.db.disconnect();
      await client.db.insert(todosTable, { title: "shutdown-check", done: false });
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
