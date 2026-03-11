import { describe, expect, it } from "vitest";
import {
  allTodosQuery,
  createNapiFjallTestEnv,
  readRowDone,
  readRowTitle,
  todoValues,
} from "./napi.fjall.test-helpers.js";

const env = createNapiFjallTestEnv();

describe("db.all NAPI Fjall integration", () => {
  it("queries durable rows from a Fjall-backed runtime", async () => {
    const store = await env.createPersistentStore("query-durable");
    const { client } = await env.openPersistentClient(store);

    const first = await client.createDurable("todos", todoValues("Task A", false), {
      tier: "worker",
    });
    const second = await client.createDurable("todos", todoValues("Task B", true), {
      tier: "worker",
    });

    const rows = await env.waitForRows(client, allTodosQuery, (entries) => entries.length === 2);
    const titles = rows.map(readRowTitle).sort();

    expect(titles).toEqual(["Task A", "Task B"]);
    expect(rows.find((row) => row.id === first.id)).toBeDefined();
    expect(rows.find((row) => row.id === second.id)).toBeDefined();
    expect(readRowDone(rows.find((row) => row.id === first.id)!)).toBe(false);
    expect(readRowDone(rows.find((row) => row.id === second.id)!)).toBe(true);
  }, 20_000);

  it("applies durable update and delete operations against Fjall storage", async () => {
    const store = await env.createPersistentStore("update-delete");
    const { client } = await env.openPersistentClient(store);

    const survivor = await client.createDurable("todos", todoValues("survivor", false), {
      tier: "worker",
    });
    const removed = await client.createDurable("todos", todoValues("removed", false), {
      tier: "worker",
    });

    await client.updateDurable(
      survivor.id,
      {
        title: { type: "Text", value: "survivor-updated" },
        done: { type: "Boolean", value: true },
      },
      { tier: "worker" },
    );
    await client.deleteDurable(removed.id, { tier: "worker" });

    const rows = await env.waitForRows(
      client,
      allTodosQuery,
      (entries) =>
        entries.length === 1 &&
        entries[0]?.id === survivor.id &&
        readRowTitle(entries[0]) === "survivor-updated" &&
        readRowDone(entries[0]) === true,
    );

    expect(rows).toHaveLength(1);
    expect(rows[0]?.id).toBe(survivor.id);
  }, 20_000);

  it("reopens a Fjall store with the latest durable state", async () => {
    const store = await env.createPersistentStore("reopen-state");
    const initial = await env.openPersistentClient(store);

    const persisted = await initial.client.createDurable("todos", todoValues("persist-me", false), {
      tier: "worker",
    });
    const removed = await initial.client.createDurable("todos", todoValues("remove-me", false), {
      tier: "worker",
    });

    await initial.client.updateDurable(
      persisted.id,
      {
        title: { type: "Text", value: "persist-me-updated" },
        done: { type: "Boolean", value: true },
      },
      { tier: "worker" },
    );
    await initial.client.deleteDurable(removed.id, { tier: "worker" });

    await env.waitForRows(
      initial.client,
      allTodosQuery,
      (entries) =>
        entries.length === 1 &&
        entries[0]?.id === persisted.id &&
        readRowTitle(entries[0]) === "persist-me-updated" &&
        readRowDone(entries[0]) === true,
    );

    await initial.shutdown();

    const reopened = await env.openPersistentClient(store);
    const reopenedRows = await env.waitForRows(
      reopened.client,
      allTodosQuery,
      (entries) =>
        entries.length === 1 &&
        entries[0]?.id === persisted.id &&
        readRowTitle(entries[0]) === "persist-me-updated" &&
        readRowDone(entries[0]) === true,
    );

    expect(reopenedRows).toHaveLength(1);
    expect(reopenedRows[0]?.id).toBe(persisted.id);

    const appended = await reopened.client.createDurable(
      "todos",
      todoValues("after-reopen", false),
      {
        tier: "worker",
      },
    );
    const finalRows = await env.waitForRows(
      reopened.client,
      allTodosQuery,
      (entries) => entries.length === 2 && entries.some((row) => row.id === appended.id),
    );

    expect(finalRows.map(readRowTitle).sort()).toEqual(["after-reopen", "persist-me-updated"]);
  }, 25_000);
});
