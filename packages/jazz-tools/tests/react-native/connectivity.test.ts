import { expect, it } from "vitest";
import { schema } from "../../src/schema-namespace.js";
import { ReadTier } from "../../src/runtime/client.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({ notes: schema.table({ title: schema.string() }) });

it("rejects disconnect without a native upstream and leaves local reads usable", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    await expect(db.disconnect()).rejects.toThrow("requires a configured serverUrl");
    const row = await db.insert(app.notes, { title: "still local" }).wait({ tier: "local" });
    expect(await db.all(app.notes, { tier: "local" })).toEqual([row]);
  });
});

it("disconnects before any query and reconnects using only native credentials", async () => {
  const { startLocalJazzServer, startTestJwtIssuer, deploy, mergePermissionsIntoWasmSchema } =
    await import("../../src/testing/index.js");
  const issuer = await startTestJwtIssuer();
  const server = await startLocalJazzServer({
    inMemory: true,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
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
    await withNativeRelayFixture(
      { wasmSchema: mergePermissionsIntoWasmSchema(app.wasmSchema, permissions) },
      async (fixture) => {
        const db = await fixture.createDb();
        // No schema runtime exists yet: offline must still stop the admitted socket.
        await db.disconnect();
        const write = db.insert(app.notes, { title: "before first query" });
        const row = await write.wait({ tier: "local" });
        let globallyAccepted = false;
        const global = write.wait({ tier: "global" }).then(() => {
          globallyAccepted = true;
        });
        expect(await db.all(app.notes, { tier: ReadTier.RemoteIfPossible })).toEqual([row]);
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
        expect(await db.all(app.notes, { tier: ReadTier.RemoteIfPossible })).toEqual([row]);
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
        session: {
          issuer: issuer.issuer,
          user_id: "connectivity-user",
          claims: {},
          authMode: "external",
        },
        upstream: { serverUrl: server.url, jwt: issuer.jwtForUser("connectivity-user") },
      },
    );
  } finally {
    await server.stop();
    await issuer.stop();
  }
}, 30_000);
