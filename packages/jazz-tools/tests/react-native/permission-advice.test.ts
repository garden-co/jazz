import { expect, it } from "vitest";
import { schema } from "../../src/schema-namespace.js";
import { ReadTier } from "../../src/runtime/client.js";
import { withNativeRelayFixture } from "./fixture.js";
import { encodeNativeForegroundCommand, decodeNativeForegroundResponse } from "jazz-rn/relay";

const app = schema.defineApp({ notes: schema.table({ title: schema.string() }) });
const permissions = schema.definePermissions(app, ({ policy, session }) => [
  policy.notes.allowRead.where({ $createdBy: session.user }),
  policy.notes.allowInsert.always(),
  policy.notes.allowUpdate.always(),
  policy.notes.allowDelete.never(),
]);

// Opaque operation ownership and explicit cancellation are below the public
// can* promise API; exercise the real native byte boundary, not a fake waiter.
it("isolates pending advice handles and retires them on cancel and close", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const first = fixture.nativeHost.openAttached(fixture.capability);
    const second = fixture.nativeHost.openAttached(fixture.capability);
    const command = encodeNativeForegroundCommand({
      type: "permissionAdvice",
      action: {
        type: "read",
        table: "notes",
        rowId: new Uint8Array(16),
      },
    });
    try {
      const response = decodeNativeForegroundResponse(first.execute(command));
      expect(response.type).toBe("pending");
      if (response.type !== "pending") throw new Error("advice did not suspend");
      const cancel = encodeNativeForegroundCommand({
        type: "cancel",
        operation: response.operation,
      });
      expect(decodeNativeForegroundResponse(second.execute(cancel))).toEqual({
        type: "cancelled",
        cancelled: false,
      });
      expect(decodeNativeForegroundResponse(first.execute(cancel))).toEqual({
        type: "cancelled",
        cancelled: true,
      });
      expect(decodeNativeForegroundResponse(first.execute(cancel))).toEqual({
        type: "cancelled",
        cancelled: false,
      });
      const closing = decodeNativeForegroundResponse(first.execute(command));
      expect(closing.type).toBe("pending");
      first.close();
      expect(() => first.execute(command)).toThrow();
      const db = await fixture.createDb();
      const row = await db
        .insert(app.notes, { title: "other foreground lives" })
        .wait({ tier: "local" });
      expect(await db.all(app.notes)).toEqual([row]);
    } finally {
      first.close();
      second.close();
    }
  });
});

it("keeps local and spoofed permission advice unknown without an authority", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb({
      ...fixture.config,
      cookieSession: {
        issuer: "https://spoof.example",
        user_id: "spoofed",
        claims: {},
        authMode: "external",
      },
    });
    const row = await db.insert(app.notes, { title: "local" }).wait({ tier: "local" });
    expect(
      await Promise.all([
        db.canInsert(app.notes, { title: "dry run" }),
        db.canRead(app.notes, row.id),
        db.canUpdate(app.notes, row.id, { title: "dry run" }),
        db.canDelete(app.notes, row.id),
      ]),
    ).toEqual(["unknown", "unknown", "unknown", "unknown"]);
    expect(await db.all(app.notes)).toEqual([row]);
  });
});

it("uses authority dry runs and admitted identity across scopes, disconnect, and shutdown", async () => {
  const { startLocalJazzServer, startTestJwtIssuer, deploy, mergePermissionsIntoWasmSchema } =
    await import("../../src/testing/index.js");
  const issuer = await startTestJwtIssuer();
  const server = await startLocalJazzServer({
    inMemory: true,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
  const nativeApp = { wasmSchema: mergePermissionsIntoWasmSchema(app.wasmSchema, permissions) };
  const options = (user: string) => ({
    appId: server.appId,
    session: { issuer: issuer.issuer, user_id: user, claims: {}, authMode: "external" as const },
    upstream: { serverUrl: server.url, jwt: issuer.jwtForUser(user) },
  });
  try {
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    await withNativeRelayFixture(
      nativeApp,
      async (alice) => {
        const db = await alice.createDb();
        await db.all(app.notes, { tier: ReadTier.Remote });
        expect(await db.canInsert(app.notes, { title: "dry run" })).toBe("allowed");
        expect(await db.all(app.notes, { tier: ReadTier.Remote })).toEqual([]);
        const row = await db.insert(app.notes, { title: "original" }).wait({ tier: "global" });
        expect(await db.canRead(app.notes, row.id)).toBe("allowed");
        expect(await db.canUpdate(app.notes, row.id, { title: "dry run" })).toBe("allowed");
        expect(await db.canDelete(app.notes, row.id)).toBe("denied");
        expect(await db.all(app.notes, { tier: ReadTier.Remote })).toEqual([row]);
        await withNativeRelayFixture(
          nativeApp,
          async (bob) => {
            const other = await bob.createDb({
              ...bob.config,
              cookieSession: options("alice").session,
            });
            await other.all(app.notes, { tier: ReadTier.Remote });
            expect(await other.canRead(app.notes, row.id)).toBe("denied");
            expect(await other.canInsert(app.notes, { title: "dry run" })).toBe("allowed");
          },
          options("bob"),
        );
        await db.disconnect();
        const pending = db.canRead(app.notes, row.id);
        const write = db.update(app.notes, row.id, { title: "survives advice cancellation" });
        await write.wait({ tier: "local" });
        expect(await pending).toBe("unknown");
        await db.reconnect();
        await write.wait({ tier: "global" });
        expect(await db.canRead(app.notes, row.id)).toBe("allowed");
        await db.disconnect();
        const closing = db.canRead(app.notes, row.id);
        await db.shutdown();
        expect(await closing).toBe("unknown");
      },
      options("alice"),
    );
  } finally {
    await server.stop();
    await issuer.stop();
  }
}, 30_000);
