import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { serializeSchemaSource } from "../../src/drivers/schema-wire.js";
import { expect, it } from "vitest";
import { schema } from "../../src/schema-namespace.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({ notes: schema.table({ title: schema.string() }) });

it("uses the native admitted identity even when caller session metadata disagrees", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb({
      ...fixture.config,
      cookieSession: {
        issuer: "https://other.example",
        user_id: "other",
        claims: {},
        authMode: "external",
      },
    });
    expect(db.getAuthState().session?.user).toBe(
      JSON.stringify(["https://auth.example", "rn-api-test"]),
    );
    expect(db.getAuthState().session?.claims).toEqual({});
  });
});

it("opens with only the native capability and no JavaScript credential", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb({
      appId: fixture.config.appId,
      nativeRelay: { capability: fixture.capability },
    });
    expect(db.getAuthState().session?.user).toBe(
      JSON.stringify(["https://auth.example", "rn-api-test"]),
    );
    expect(await db.all(app.notes, { tier: "local" })).toEqual([]);
  });
});

it("revokes the old foreground before opening an isolated newly admitted identity", async () => {
  await withNativeRelayFixture(app, async (first) => {
    const old = await first.createDb();
    await old.insert(app.notes, { title: "first identity private row" }).wait({ tier: "local" });
    first.nativeHost.revoke(first.capability);
    await expect(old.all(app.notes, { tier: "local" })).rejects.toThrow();
    await expect(first.createDb()).rejects.toThrow();
    const newCapability = first.nativeHost.admit(
      JSON.stringify({
        scope: {
          app_namespace: first.config.appId,
          storage_namespace: "default",
          auth_scope: "second-identity",
        },
        sqlite_path: join(first.directory, "second.sqlite"),
        schema_json: serializeSchemaSource(app.wasmSchema),
        identity: {
          node: randomUUID(),
          author: JSON.stringify(["https://auth.example", "second"]),
        },
        claims: {},
      }),
    );
    const current = await first.createDb({
      ...first.config,
      nativeRelay: { capability: newCapability },
    });
    expect(current.getAuthState().session?.user).toBe(
      JSON.stringify(["https://auth.example", "second"]),
    );
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
    await db.logout();
    await Promise.all([db.shutdown(), db.shutdown()]);
    expect(await db.all(app.notes, { tier: "local" })).toEqual([]);
    const reopened = await fixture.createDb();
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
