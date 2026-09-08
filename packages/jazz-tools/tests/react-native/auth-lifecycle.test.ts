import { expect, it } from "vitest";
import { mergePermissionsIntoWasmSchema } from "../../src/schema-permissions.js";
import { schema } from "../../src/schema-namespace.js";
import { ReadTier } from "../../src/runtime/client.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({ notes: schema.table({ title: schema.string() }) });

it("rejects caller credentials and requires an enrolled account handle", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    await expect(
      fixture.createDb({
        ...fixture.config,
        // @ts-expect-error Raw credentials cannot override an account handle.
        cookieSession: { issuer: "https://other.example", user_id: "other", claims: {} },
      }),
    ).rejects.toThrow("account_handle_required");
    await expect(
      // @ts-expect-error A native capability is not a public account handle.
      fixture.createDb({
        appId: fixture.config.appId,
        nativeRelay: { capability: fixture.capability },
      }),
    ).rejects.toThrow();
    const db = await fixture.createDb();
    expect(db.getAuthState().session?.user).toEqual({
      account: fixture.config.account.id,
      identity: fixture.config.account.identity,
    });
    expect(await db.all(app.notes, { tier: "local" })).toEqual([]);
  });
});

it("logout retires old contexts before opening an independently registered identity", async () => {
  await withNativeRelayFixture(app, async (first) => {
    const old = await first.createDb();
    await old.insert(app.notes, { title: "first identity private row" }).wait({ tier: "local" });
    const pending = old.all(app.notes, { tier: ReadTier.Remote });
    const pendingRejected = expect(pending).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 30));
    const transaction = old.beginTransaction();
    transaction.insert(app.notes, { title: "abandoned old-scope mutation" });
    await first.manager.logout();
    await pendingRejected;
    await expect(
      Promise.resolve().then(() => transaction.commit().wait({ tier: "local" })),
    ).rejects.toThrow();
    await expect(old.all(app.notes, { tier: "local" })).rejects.toThrow();
    await expect(first.createDb()).rejects.toThrow();
    const secondConfig = await first.registerIdentity("second");
    const current = await first.createDb(secondConfig);
    await expect(old.all(app.notes, { tier: "local" })).rejects.toThrow();
    expect(current.getAuthState().session?.user).toEqual({
      account: secondConfig.account.id,
      identity: secondConfig.account.identity,
    });
    expect(await current.all(app.notes, { tier: "local" })).toEqual([]);
    await current.insert(app.notes, { title: "second identity row" }).wait({ tier: "local" });
    expect((await current.all(app.notes, { tier: "local" })).map((row) => row.title)).toEqual([
      "second identity row",
    ]);
  });
});

it("logout and repeated shutdown retire the public foreground", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    await db.insert(app.notes, { title: "retained on logout" }).wait({ tier: "local" });
    await fixture.manager.logout();
    await Promise.all([db.shutdown(), db.shutdown()]);
    await expect(db.all(app.notes, { tier: "local" })).rejects.toThrow("shutting down or closed");
    expect(() => db.insert(app.notes, { title: "must not reopen" })).toThrow(
      "shutting down or closed",
    );
    expect(() => db.subscribe(app.notes, () => {})).toThrow("shutting down or closed");
    expect(() => db.beginTransaction()).toThrow("shutting down or closed");
    const reopened = await fixture.createDb(await fixture.loginOriginal());
    await expect
      .poll(async () => (await reopened.all(app.notes, { tier: "local" })).map((row) => row.title))
      .toEqual(["retained on logout"]);
  });
});

it("rejects auth replacement before and after first query without changing public identity", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    const admitted = db.getAuthState();
    for (const materialized of [false, true]) {
      if (materialized) await db.all(app.notes, { tier: "local" });
      expect(() => db.updateAuthToken(null)).toThrow("native-admission bound");
      expect(() =>
        db.updateCookieSession({
          issuer: "https://other.example",
          user_id: "other",
          claims: {},
          authMode: "external",
        }),
      ).toThrow("native-admission bound");
      expect(db.getAuthState()).toEqual(admitted);
    }
  });
});

it("rejects operations once shutdown starts even before a runtime was materialized", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    const closing = db.shutdown();
    await expect(db.all(app.notes, { tier: "local" })).rejects.toThrow("shutting down or closed");
    expect(() => db.insert(app.notes, { title: "must not initialize" })).toThrow(
      "shutting down or closed",
    );
    await closing;
    await db.shutdown();
  });
});

// A typed native liveness receipt cannot be requested through a public data
// query: this boundary check uses the real host lease, never a mocked runtime.
it("distinguishes a live native foreground from a revoked native handle", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const runtime = fixture.nativeHost.openAttached(fixture.capability);
    try {
      expect(runtime.isClosed?.()).toBe(false);
      fixture.nativeHost.revoke(fixture.capability);
      expect(runtime.isClosed?.()).toBe(true);
    } finally {
      runtime.close();
    }
  });
});

it("caller config mutation cannot replace the handle behind an existing context", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const supplied = { ...fixture.config };
    const first = await fixture.createDb(supplied);
    const secondConfig = await fixture.registerIdentity("review-second");
    supplied.account = secondConfig.account;
    await first.insert(app.notes, { title: "belongs to first admission" }).wait({ tier: "local" });
    expect(first.getAuthState().session?.user).toEqual({
      account: fixture.config.account.id,
      identity: fixture.config.account.identity,
    });
    const other = await fixture.createDb(secondConfig);
    const original = await fixture.createDb();
    await expect
      .poll(async () => (await original.all(app.notes, { tier: "local" })).map((row) => row.title))
      .toEqual(["belongs to first admission"]);
    expect(await other.all(app.notes, { tier: "local" })).toEqual([]);
  });
});

it("retains external provider claims when borrowing an admitted native account", async () => {
  const scopedApp = schema.defineApp({
    notes: schema.table({ title: schema.string(), role: schema.string() }),
  });
  const permissions = schema.definePermissions(scopedApp, ({ policy, session }) => {
    policy.notes.allowRead.where({ role: session.claims["role"] });
  });
  await withNativeRelayFixture(
    { wasmSchema: mergePermissionsIntoWasmSchema(scopedApp.wasmSchema, permissions) },
    async (fixture) => {
      const db = await fixture.createDb({
        ...fixture.config,
        nativeRelay: { capability: fixture.capability },
      });
      const role = db.getAuthState().session?.claims.role;
      expect(role).toBe("member");
      if (typeof role !== "string") throw new Error("provider role missing");
      // Local-first reads expose local knowledge. The app can use the retained
      // claim to construct its own view; only the authority enforces policies.
      const visible = scopedApp.notes.where({ role });
      const written = await db
        .insert(scopedApp.notes, { title: "member note", role: "member" })
        .wait({ tier: "local" });
      await db
        .insert(scopedApp.notes, { title: "admin note", role: "admin" })
        .wait({ tier: "local" });
      expect(await db.all(visible)).toMatchObject([{ id: written.id, title: "member note" }]);
      const snapshots: unknown[][] = [];
      const stop = db.subscribe(visible, (rows) => snapshots.push(rows));
      try {
        await expect
          .poll(() => snapshots.at(-1))
          .toMatchObject([{ id: written.id, title: "member note" }]);
      } finally {
        stop();
      }
    },
    {
      session: {
        issuer: "https://auth.example",
        user_id: "member",
        claims: { role: "member" },
        authMode: "external",
      },
    },
  );
});
