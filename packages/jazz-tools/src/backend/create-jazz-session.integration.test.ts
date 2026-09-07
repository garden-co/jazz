import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { createJazzSession } from "./index.js";

const app = s.defineApp({
  notes: s.table({ text: s.string() }),
  posts: s.table({ text: s.string() }),
});
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.posts.allowRead.always();
  policy.posts.allowInsert.always();
  policy.posts.allowUpdate.always();
  policy.posts.allowDelete.always();
  policy.notes.allowRead.always();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});

describe("Node shared backend session", () => {
  it("rejects invalid service admission before publishing a client", async () => {
    const appId = randomUUID();
    const server = await startLocalJazzServer({ appId, backendSecret: "expected-service-secret" });
    try {
      await expect(
        createJazzSession({
          appId,
          serverUrl: server.url,
          app,
          driver: { type: "memory" },
          initial: { backendSecret: "incorrect-service-secret" },
        }),
      ).rejects.toThrow("Backend admission failed (401)");
    } finally {
      await server.stop();
    }
  });

  it("retains the memory node clock when a failed transition reopens its previous handle", async () => {
    const appId = randomUUID();
    const backendSecret = "clock-service-secret";
    const server = await startLocalJazzServer({ appId, backendSecret });
    const owner = await createJazzSession({
      appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: { backendSecret },
    });
    try {
      const previous = owner.getSnapshot();
      // Native wall time cannot be frozen by JS timers. Seed a future native
      // clock to make lost high-water observable even after network round trips.
      const native = (client: typeof previous.client) =>
        (
          client!.db as unknown as { client: { getRuntime(): NativeRuntimeAdapter } }
        ).client.getRuntime();
      const floor = 1n << 62n;
      native(previous.client).seedForegroundTxTimeHighWater(floor);
      await expect(owner.becomeBackend({ backendSecret: "wrong-clock-secret" })).rejects.toThrow(
        "401",
      );
      const restored = owner.getSnapshot();
      expect(restored.status).toBe("ready");
      expect(restored.account).toBe(previous.account);
      expect(restored.client).not.toBe(previous.client);
      expect(native(restored.client).foregroundTxTimeHighWater()).toBeGreaterThanOrEqual(floor);
    } finally {
      await owner.close();
      await server.stop();
    }
  });

  it("keeps unsynced scoped writes fenced until reconnect allows transition", async () => {
    const appId = randomUUID();
    const backendSecret = "pending-service-secret";
    const server = await startLocalJazzServer({ appId, backendSecret });
    const owner = await createJazzSession({
      appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: { backendSecret },
    });
    const user = await createJazzSession({
      appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: "local-first",
    });
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret: server.adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });
      const backend = owner.getSnapshot().client!;
      const scoped = await backend.forAccount(user.getSnapshot().account!);
      await backend.db.disconnect();
      const row = await scoped.insert(app.posts, { text: "pending scope" }).wait({ tier: "local" });
      let finished = false;
      const transition = owner.createLocalFirst().finally(() => {
        finished = true;
      });
      await vi.waitFor(() => expect(owner.getSnapshot().status).toBe("transitioning"));
      await new Promise((resolve) => setTimeout(resolve, 25));
      expect(finished).toBe(false);
      await expect(scoped.shutdown({ waitForSync: true })).rejects.toThrow(/sync|shut/i);
      expect(() => scoped.insert(app.posts, { text: "race" })).toThrow(/shut|closed/);
      await backend.db.reconnect();
      await transition;
      expect(owner.getSnapshot().account?.identity.issuer).toBe("urn:jazz:local-first");
      expect(
        await owner.getSnapshot().client!.db.one(app.posts.where({ id: row.id }), { tier: "edge" }),
      ).toMatchObject({ text: "pending scope" });
    } finally {
      await owner.close();
      await user.close();
      await server.stop();
    }
  }, 30_000);

  it("preserves SYSTEM provenance, drops authority on transition, and isolates user scopes", async () => {
    const appId = randomUUID();
    const backendSecret = "session-service-secret";
    const adminSecret = "session-publication-secret";
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const session = await createJazzSession({
      appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
    });
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });
      await session.becomeBackend({ backendSecret });
      const first = session.getSnapshot();
      expect(first.status).toBe("ready");
      expect(first.account?.id).toBe("00000000-0000-0000-0000-000000000000");
      expect(first.account?.identity.issuer).toBe("urn:jazz:system");
      expect(JSON.stringify(first.account)).not.toContain(backendSecret);
      const backend = first.client!;
      expect(backend.session?.user).toEqual({
        account: first.account!.id,
        identity: first.account!.identity,
      });
      const initial = await backend.db
        .insert(app.notes, { text: "service" })
        .wait({ tier: "edge" });
      expect(
        await backend.db.one(app.notes.select("$createdBy").where({ id: initial.id })),
      ).toMatchObject({ $createdBy: backend.session?.user });

      await session.createLocalFirst();
      const user = session.getSnapshot();
      await expect(backend.forAccount(user.account!)).rejects.toThrow(/closed|shut down/);
      expect(() => backend.db.insert(app.notes, { text: "stale service" })).toThrow(
        /closed|shut down/,
      );
      await expect(user.client!.forRequest({ headers: {} })).rejects.toThrow("backend account");
      expect(
        await user.client!.db.one(app.notes.where({ id: initial.id }), { tier: "edge" }),
      ).toMatchObject({ text: "service" });
      await user.client!.db.insert(app.posts, { text: "ordinary positive" }).wait({ tier: "edge" });
      await expect(async () => {
        await user.client!.db.insert(app.notes, { text: "denied user" }).wait({ tier: "edge" });
      }).rejects.toThrow(/permission|denied|policy/i);

      await session.becomeBackend({ backendSecret });
      const current = session.getSnapshot().client!;
      const scoped = await current.forAccount(user.account!);
      await expect(async () => {
        await scoped.insert(app.notes, { text: "denied scoped user" }).wait({ tier: "edge" });
      }).rejects.toThrow(/permission|denied|policy/i);
      const other = await createJazzSession({
        appId,
        serverUrl: server.url,
        app,
        permissions,
        driver: { type: "memory" },
        initial: "local-first",
      });
      try {
        const otherAccount = other.getSnapshot().account!;
        const otherScope = await current.forAccount(otherAccount);
        const rows = await Promise.all([
          scoped.insert(app.posts, { text: "first scope" }).wait({ tier: "edge" }),
          otherScope.insert(app.posts, { text: "second scope" }).wait({ tier: "edge" }),
        ]);
        for (const [index, expected] of [user.account!, otherAccount].entries()) {
          expect(
            await current.db.one(app.posts.select("$createdBy").where({ id: rows[index]!.id })),
          ).toMatchObject({ $createdBy: { account: expected.id, identity: expected.identity } });
        }
      } finally {
        await other.close();
      }
      const attributed = await current.withAttribution(user.account!);
      const row = await attributed
        .insert(app.notes, { text: "attributed service" })
        .wait({ tier: "edge" });
      expect(
        await current.db.one(app.notes.select("$createdBy").where({ id: row.id })),
      ).toMatchObject({
        $createdBy: { account: user.account!.id, identity: user.account!.identity },
      });
      expect(session.getSnapshot().account?.identity.issuer).toBe("urn:jazz:system");
      await session.close();
      expect(() => scoped.insert(app.notes, { text: "closed scope" })).toThrow(/closed|shut down/);
      await expect(current.forAccount(user.account!)).rejects.toThrow(/closed|shut down/);
    } finally {
      await session.close();
      await server.stop();
    }
  }, 30_000);
});
