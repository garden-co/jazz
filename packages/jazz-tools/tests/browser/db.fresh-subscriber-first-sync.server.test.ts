import { afterEach, describe, expect, it } from "vitest";
import { generateAuthSecret, type Db } from "../../src/index.js";
import { app } from "./apps/fresh-subscriber-first-sync/jazz.generated.js";
import permissions from "./apps/fresh-subscriber-first-sync/jazz.permissions.json";
import { TestCleanup, createBrowserTestDb, uniqueDbName, waitForCondition } from "./support.js";
import { deploy, getJazzServerInfo, startJazzApp } from "./testing-server.js";

// The app (apps/fresh-subscriber-first-sync): docs every user may read and insert.

const ctx = new TestCleanup();
afterEach(async () => {
  await ctx.cleanup();
});

describe("fresh subscriber first sync", () => {
  // Applying a first result of a few MB spans more than one bounded evaluation
  // turn; the host must keep driving it until the rows are published.
  it("delivers a multi-MB first result to a fresh global subscriber", async () => {
    const {
      appId,
      serverUrl: coreUrl,
      adminSecret,
    } = await getJazzServerInfo(uniqueDbName("fresh-subscriber-first-sync"));
    await deploy({
      appId,
      serverUrl: coreUrl,
      adminSecret,
      app,
      permissions: permissions,
    });
    const { serverUrl } = await startJazzApp(
      { appId, serverUrl: coreUrl, adminSecret },
      { schema: app, permissions },
    );
    const secret = generateAuthSecret();
    const open = async (label: string): Promise<Db> =>
      ctx.track(
        await createBrowserTestDb({
          appId,
          serverUrl,
          secret,
          driver: { type: "persistent", dbName: uniqueDbName(label) },
        }),
      );
    const rowCount = 100;
    const body = "x".repeat(60_000);
    const writer = await open("first-sync-writer");
    for (let start = 0; start < rowCount; start += 20) {
      await Promise.all(
        Array.from({ length: 20 }, (_, offset) =>
          writer
            .insert(app.docs, { label: `doc-${start + offset}`, body })
            .wait({ tier: "global" }),
        ),
      );
    }

    const reader = await open("first-sync-reader");
    let rows: { label: string; body: string }[] = [];
    let error: unknown;
    ctx.trackSubscription(
      reader.subscribe(
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
      ),
    );
    await waitForCondition(
      async () => error !== undefined || rows.length === rowCount,
      30_000,
      "fresh subscriber did not receive its multi-MB first result",
    );
    expect(error).toBeUndefined();
    expect(new Set(rows.map((row) => row.label))).toEqual(
      new Set(Array.from({ length: rowCount }, (_, index) => `doc-${index}`)),
    );
    expect(rows.every((row) => row.body === body)).toBe(true);
  }, 90_000);
});
