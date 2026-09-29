import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "./index.js";

const app = s.defineApp({
  docs: s.table({ label: s.string(), body: s.string() }, {}),
});
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.docs.allowRead.always();
  policy.docs.allowInsert.always();
});

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
      allowLocalFirstAuth: true,
    };
    const rowCount = 100;
    const body = "x".repeat(60_000);
    const server = await startLocalJazzServer({ ...settings, schema: app, permissions });
    const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
    const open = async (name: string) => {
      const session = await createJazzSession({
        appId: settings.appId,
        app,
        permissions,
        serverUrl: server.url,
        driver: { type: "persistent", dataPath: join(directory, name) },
        initial: "local-first",
      });
      sessions.push(session);
      const snapshot = session.getSnapshot();
      if (!snapshot.client) throw snapshot.error ?? new Error(snapshot.status);
      return snapshot.client.db;
    };
    let unsubscribe: (() => void) | undefined;
    try {
      await deploy({ ...settings, schema: app, permissions, serverUrl: server.url });
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
      let rows: { label: string; body: string }[] | undefined;
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
        { tier: "global" },
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
