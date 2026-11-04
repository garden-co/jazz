import { expect, it } from "vitest";
import { schema } from "../../src/schema-namespace.js";
import { ReadTier } from "../../src/runtime/client.js";
import * as nativeCommands from "jazz-rn/relay";
import type { NativeForegroundModule } from "../../src/react-native/native-foreground-db.js";
const { encodeNativeForegroundCommand, decodeNativeForegroundResponse } =
  nativeCommands as unknown as NativeForegroundModule;
import { withNativeRelayFixture } from "./fixture.js";
import type { TestJazz } from "../../src/testing/index.js";

const app = schema.defineApp(
  { notes: schema.table({ title: schema.string() }, {}) },
  { migrations: "none" },
);

it("rejects disconnect without a native upstream and leaves local reads usable", async () => {
  await withNativeRelayFixture(app, {}, async (fixture) => {
    const db = await fixture.createDb();
    await expect(db.disconnect()).rejects.toThrow("requires a configured serverUrl");
    const row = await db.insert(app.notes, { title: "still local" }).wait({ tier: "local" });
    expect(await db.all(app.notes, { tier: "local-first" })).toEqual([row]);
  });
});

it("disconnects before any query and reconnects using only native credentials", async () => {
  const { startLocalJazzServer, startTestJazz, deploy } =
    await import("../../src/testing/index.js");
  const server = await startLocalJazzServer({
    inMemory: true,
  });
  let backend: TestJazz | undefined;
  try {
    const permissions = schema.definePermissions(app, ({ policy }) => [
      policy.notes.allowRead.always(),
      policy.notes.allowInsert.always(),
    ]);
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    backend = await startTestJazz({ core: server, app, permissions });
    await withNativeRelayFixture(
      app,
      permissions,
      async (fixture) => {
        const db = await fixture.createDb();
        const nativeStatus = () => {
          const foreground = fixture.nativeHost.openAttached(fixture.capability);
          try {
            return decodeNativeForegroundResponse(
              foreground.execute(encodeNativeForegroundCommand({ type: "nativeConnectionStatus" })),
            );
          } finally {
            foreground.close();
          }
        };
        await expect
          .poll(nativeStatus)
          .toMatchObject({ type: "nativeConnectionStatus", connected: true });
        // No schema runtime exists yet: offline must fence its first native use.
        await db.disconnect();
        const write = db.insert(app.notes, { title: "before first query" });
        const row = await write.wait({ tier: "local" });
        // Account contexts defer native schema attachment until first use. The
        // pending disconnect is applied before that foreground performs work.
        expect(nativeStatus()).toEqual({
          type: "nativeConnectionStatus",
          configured: true,
          explicitlyOffline: true,
          connected: false,
          authRejected: null,
        });
        let globallyAccepted = false;
        const global = write.wait({ tier: "global" }).then(() => {
          globallyAccepted = true;
        });
        expect(
          await db.all(app.notes, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 60_000 }),
        ).toEqual([row]);
        let settled = false;
        const remote = db.all(app.notes, { tier: ReadTier.Remote }).then((rows) => {
          settled = true;
          return rows;
        });
        await new Promise((resolve) => setTimeout(resolve, 100));
        expect(settled).toBe(false);
        expect(globallyAccepted).toBe(false);
        await db.reconnect();
        const opening = await remote;
        expect(opening.every((candidate) => candidate.id === row.id)).toBe(true);
        await global;
        expect(await db.all(app.notes, { tier: ReadTier.Remote })).toEqual([row]);
        await Promise.all([db.disconnect(), db.reconnect(), db.disconnect()]);
        expect(
          await db.all(app.notes, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 60_000 }),
        ).toEqual([row]);
        await db.reconnect();
        expect(await db.all(app.notes, { tier: ReadTier.Remote })).toEqual([row]);
        await db.disconnect();
        const cancelledRead = db.all(app.notes, { tier: ReadTier.Remote });
        const rejected = expect(cancelledRead).rejects.toThrow("shutting down or closed");
        await db.shutdown();
        await rejected;
      },
      {
        appId: server.appId,
        upstream: { backend: backend!, userId: "connectivity-user" },
      },
    );
  } finally {
    await backend?.stop();
    await server.stop();
  }
}, 30_000);
