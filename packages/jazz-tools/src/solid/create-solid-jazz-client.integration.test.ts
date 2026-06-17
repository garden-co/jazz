import { randomUUID } from "node:crypto";
import { createRoot } from "solid-js";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import {
  createSolidJazzClient,
  isPendingSolidJazzClientReady,
  type SolidJazzClient,
} from "./create-solid-jazz-client.js";

const app = s.defineApp({
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
});

function makeAppId(scope: string): string {
  return `solid-create-jazz-client-${scope}-${randomUUID()}`;
}

async function waitForReady(
  result: ReturnType<typeof createSolidJazzClient>,
): Promise<SolidJazzClient> {
  for (let i = 0; i < 500; i += 1) {
    const error = result.error;
    if (error) {
      throw error;
    }
    if (isPendingSolidJazzClientReady(result)) {
      return result;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("Timed out waiting for Solid Jazz client to become ready");
}

describe("solid/create-jazz-client integration", () => {
  it("SD-I01: supports insert followed by query through returned db", async () => {
    let client: SolidJazzClient | null = null;
    let dispose: () => void = () => {};

    try {
      const result = createRoot((rootDispose) => {
        dispose = rootDispose;
        return createSolidJazzClient(() => ({
          appId: makeAppId("mutation-query"),
        }));
      });
      client = await waitForReady(result);

      const inserted = await client.db
        .insert(app.todos, {
          title: "buy milk",
          done: false,
        })
        .wait({
          tier: "local",
        });
      const rows = await client.db.all(app.todos.where({}));

      expect(
        rows.some(
          (row) => row.id === inserted.id && row.title === "buy milk" && row.done === false,
        ),
      ).toBe(true);
    } finally {
      if (client) {
        await client.shutdown();
      }
      dispose();
    }
  }, 15000);

  it("SD-I02: respects caller-provided insert id", async () => {
    let client: SolidJazzClient | null = null;
    let dispose: () => void = () => {};
    const externalId = "550e8400-e29b-41d4-a716-446655440000";

    try {
      const result = createRoot((rootDispose) => {
        dispose = rootDispose;
        return createSolidJazzClient(() => ({
          appId: makeAppId("external-id"),
        }));
      });
      client = await waitForReady(result);

      const inserted = await client.db
        .insert(app.todos, { title: "with external id", done: false }, { id: externalId })
        .wait({
          tier: "local",
        });
      const rows = await client.db.all(app.todos.where({}));

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
      dispose();
    }
  }, 15000);

  it("SD-I03: shutdown succeeds after performing db activity", async () => {
    let client: SolidJazzClient | null = null;
    let dispose: () => void = () => {};

    try {
      const result = createRoot((rootDispose) => {
        dispose = rootDispose;
        return createSolidJazzClient(() => ({ appId: makeAppId("shutdown") }));
      });
      client = await waitForReady(result);

      await client.db
        .insert(app.todos, {
          title: "shutdown-check",
          done: false,
        })
        .wait({
          tier: "local",
        });
      await client.db.all(app.todos.where({}));

      await expect(client.shutdown()).resolves.toBeUndefined();
      client = null;
    } finally {
      if (client) {
        await client.shutdown();
      }
      dispose();
    }
  }, 15000);
});
