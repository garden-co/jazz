import { describe, expect, it, vi } from "vitest";
import type { RowDelta } from "../drivers/types.js";
import {
  allTodosQuery,
  createNapiFjallTestEnv,
  readRowTitle,
  todoValues,
  todosByDone,
} from "./napi.fjall.test-helpers.js";

const env = createNapiFjallTestEnv();

describe("db.subscribeAll NAPI Fjall integration", () => {
  it("emits add changes for durable inserts in Fjall storage", async () => {
    const store = await env.createPersistentStore("subscription-insert");
    const { client } = await env.openPersistentClient(store);
    const deltas: RowDelta[] = [];

    const subscriptionId = client.subscribe(
      allTodosQuery,
      (delta) => {
        deltas.push(delta);
      },
      { tier: "worker" },
    );

    try {
      await client.createDurable("todos", todoValues("watch-me", false), { tier: "worker" });

      await vi.waitFor(() => {
        expect(
          deltas.some((delta) =>
            delta.some(
              (change) =>
                change.kind === 0 && change.row && readRowTitle(change.row) === "watch-me",
            ),
          ),
        ).toBe(true);
      });
    } finally {
      client.unsubscribe(subscriptionId);
    }
  });

  it("supports condition filters for Fjall-backed subscriptions", async () => {
    const store = await env.createPersistentStore("subscription-filter");
    const { client } = await env.openPersistentClient(store);
    const deltas: RowDelta[] = [];

    const subscriptionId = client.subscribe(
      todosByDone(false),
      (delta) => {
        deltas.push(delta);
      },
      { tier: "worker" },
    );

    try {
      await client.createDurable("todos", todoValues("visible", false), { tier: "worker" });
      await client.createDurable("todos", todoValues("hidden", true), { tier: "worker" });

      await vi.waitFor(() => {
        expect(
          deltas.some((delta) =>
            delta.some(
              (change) => change.kind === 0 && change.row && readRowTitle(change.row) === "visible",
            ),
          ),
        ).toBe(true);
      });

      expect(
        deltas.some((delta) =>
          delta.some(
            (change) =>
              change.row !== undefined &&
              change.row !== null &&
              readRowTitle(change.row) === "hidden",
          ),
        ),
      ).toBe(false);

      const rows = await env.waitForRows(
        client,
        todosByDone(false),
        (entries) => entries.length === 1 && readRowTitle(entries[0]) === "visible",
      );

      expect(rows).toHaveLength(1);
      expect(readRowTitle(rows[0]!)).toBe("visible");
    } finally {
      client.unsubscribe(subscriptionId);
    }
  });
});
