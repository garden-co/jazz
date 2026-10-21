import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema } from "../index.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { createJazzClient as createReactJazzClient } from "../react/create-jazz-client.js";
import { createJazzClient as createSvelteJazzClient } from "../svelte/create-jazz-client.js";
import { createJazzClient as createVueJazzClient } from "../vue/create-jazz-client.js";
import { createJazzClient } from "./create-jazz-client.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }, {}),
});

describe("web/create-jazz-client public API", () => {
  it.each([
    ["react", createReactJazzClient],
    ["svelte", createSvelteJazzClient],
    ["vue", createVueJazzClient],
  ])("%s re-exports the canonical web client factory", (_framework, createClient) => {
    expect(createClient).toBe(createJazzClient);
  });

  it("inserts with generated and supplied IDs, queries, and shuts down after activity", async () => {
    const client = await createJazzClient(
      await localAccountConfig(`web-create-jazz-client-${randomUUID()}`),
    );
    try {
      const externalId = "550e8400-e29b-41d4-a716-446655440000";
      const generated = client.db.insert(app.todos, { title: "generated ID", done: false });
      const supplied = client.db.insert(
        app.todos,
        { title: "supplied ID", done: true },
        { id: externalId },
      );
      expect(supplied.value.id).toBe(externalId);
      expect(await client.db.all(app.todos.orderBy("title"), { tier: "local-first" })).toEqual([
        { id: generated.value.id, title: "generated ID", done: false },
        { id: externalId, title: "supplied ID", done: true },
      ]);
    } finally {
      await expect(client.shutdown()).resolves.toBeUndefined();
    }
  }, 15_000);
});
