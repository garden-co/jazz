import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { startTestJazz, testApp, testPermissions } from "../testing/index.js";
import { createJazzClient } from "./index.js";

const app = testApp({
  tables: {
    docs: { columns: { label: "string", body: "string" } },
  },
});
const permissions = {
  format: 1,
  tables: { docs: { read: "true", insert: "true" } },
};

describe("fresh subscriber first sync", () => {
  // Applying a first result of a few MB spans more than one bounded evaluation
  // turn. The Node host must keep driving that apply rather than drop it
  // part-way, which left a fresh subscriber at no rows and no error.
  it("delivers a multi-MB first result to a fresh global subscriber", async () => {
    const directory = await mkdtemp(join(tmpdir(), "jazz-first-sync-test-"));
    const settings = {
      appId: randomUUID(),
      dataDir: join(directory, "server"),
      adminSecret: randomUUID(),
      backendSecret: randomUUID(),
    };
    const rowCount = 100;
    const body = "x".repeat(60_000);
    const server = await startTestJazz({ app, permissions, coreOptions: settings });
    const sessions: Awaited<ReturnType<typeof createJazzClient>>[] = [];
    const open = async (name: string) => {
      const session = await createJazzClient({
        appId: settings.appId,
        app,
        permissions: testPermissions(app, permissions),
        serverUrl: server.url,
        driver: { type: "persistent", dataPath: join(directory, name) },
        initial: "device",
      });
      sessions.push(session);
      const snapshot = session.getSnapshot();
      if (!snapshot.client) throw snapshot.error ?? new Error(snapshot.status);
      return snapshot.client.db;
    };
    let unsubscribe: (() => void) | undefined;
    try {
      const writer = await open("writer");
      for (let start = 0; start < rowCount; start += 20) {
        await Promise.all(
          Array.from({ length: 20 }, (_, offset) =>
            writer
              .insert(app.docs, { label: `doc-${start + offset}`, body })
              .wait({ tier: "global" }),
          ),
        );
      }

      const reader = await open("reader");
      let rows: ReadonlyArray<Record<string, unknown>> | undefined;
      let error: unknown;
      unsubscribe = reader.subscribe(
        app.docs,
        {
          onUpdate: (next) => {
            rows = next;
          },
          onError: (next) => {
            error = next;
          },
        },
        { tier: "remote" },
      );
      await vi.waitFor(
        () => {
          expect(error).toBeUndefined();
          expect(rows).toHaveLength(rowCount);
        },
        { timeout: 30_000, interval: 50 },
      );
      expect(new Set(rows!.map((row) => row.label))).toEqual(
        new Set(Array.from({ length: rowCount }, (_, index) => `doc-${index}`)),
      );
      expect(rows!.every((row) => row.body === body)).toBe(true);
    } finally {
      unsubscribe?.();
      for (const session of sessions) await session.close();
      await server.stop();
      await rm(directory, { recursive: true, force: true });
    }
  }, 120_000);
});
