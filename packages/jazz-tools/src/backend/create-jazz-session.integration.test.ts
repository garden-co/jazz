import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { createJazzClient } from "./index.js";
import {
  backendDbAttributedTo,
  backendDbForSession,
  openJazzSession,
} from "./create-jazz-session.js";
import { accountRegistryUrl } from "../accounts/context.js";
import { readAccountAssignment, requestVouchedAccount } from "../accounts/registry-client.js";

const app = s.defineApp(
  {
    notes: s.table({ text: s.string() }, {}),
    posts: s.table({ text: s.string() }, {}),
  },
  { migrations: "none" },
);
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

/** A user the app's backend signed in, and the session it acts as them with. */
async function appUser(
  server: { url: string },
  appId: string,
  backendSecret: string,
  name: string,
) {
  const identity = { issuer: "https://app.session.test", subject: `${name}-${randomUUID()}` };
  const response = await requestVouchedAccount(
    accountRegistryUrl(server.url, appId),
    "login-or-register",
    identity,
    backendSecret,
  );
  const account = readAccountAssignment(response, identity);
  return {
    author: { account, identity },
    session: {
      account_id: account,
      issuer: identity.issuer,
      user_id: identity.subject,
      claims: {},
      authMode: "external" as const,
    },
  };
}

describe("Node shared backend session", () => {
  it("never opens a public client as the backend", async () => {
    const base = {
      appId: randomUUID(),
      serverUrl: "http://127.0.0.1:1",
      app,
      permissions,
      driver: { type: "memory" as const },
    };
    await expect(
      createJazzClient({ ...base, initial: { backendSecret: "secret" } } as never),
    ).rejects.toThrow('initial is "device" or left out');
    // The option check keeps webSocket's own hint.
    await expect(createJazzClient({ ...base, webSocket: class {} } as never)).rejects.toThrow(
      "Use credential instead of webSocket",
    );
    for (const key of ["backendSecret", "adminSecret"]) {
      await expect(createJazzClient({ ...base, [key]: "secret" } as never)).rejects.toThrow(
        `${key} is not a createJazzClient option`,
      );
    }
  });

  it("rejects invalid service admission before publishing a client", async () => {
    const appId = randomUUID();
    const server = await startLocalJazzServer({ appId, backendSecret: "expected-service-secret" });
    try {
      await expect(
        openJazzSession({
          appId,
          serverUrl: server.url,
          app,
          permissions,
          driver: { type: "memory" },
          initial: { backendSecret: "incorrect-service-secret" },
        }),
      ).rejects.toThrow("Backend admission failed (401)");
    } finally {
      await server.stop();
    }
  });

  it("the main backend's own client acts for a user through jazz.forReqUser, not forUser", async () => {
    const appId = randomUUID();
    const backendSecret = "for-user-service-secret";
    const server = await startLocalJazzServer({ appId, backendSecret });
    try {
      const owner = await openJazzSession({
        appId,
        serverUrl: server.url,
        app,
        permissions,
        driver: { type: "memory" },
        initial: { backendSecret },
      });
      try {
        await expect(owner.getSnapshot().client!.forUser("alice")).rejects.toThrow(
          /only on a trusted backend client.*jazz\.forReqUser/,
        );
      } finally {
        await owner.close();
      }
    } finally {
      await server.stop();
    }
  });

  it("retains the memory node clock when retry reopens the backend's client", async () => {
    const appId = randomUUID();
    const backendSecret = "clock-service-secret";
    const server = await startLocalJazzServer({ appId, backendSecret });
    const owner = await openJazzSession({
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
      await owner.retry();
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

  it("preserves SYSTEM provenance, refuses becoming a guest, and isolates user scopes", async () => {
    const appId = randomUUID();
    const backendSecret = "session-service-secret";
    const adminSecret = "session-publication-secret";
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const { bundle } = await deploy({
      serverUrl: server.url,
      appId,
      adminSecret,
      schema: resolveSchemaSource(app),
      permissions,
    });
    const session = await openJazzSession({
      appId,
      serverUrl: server.url,
      app,
      permissions,
      bundle,
      driver: { type: "memory" },
      initial: { backendSecret },
    });
    try {
      // A public session has no way to become the backend after it opened.
      expect("becomeBackend" in session).toBe(false);
      const first = session.getSnapshot();
      expect(first.status).toBe("ready");
      expect(first.account?.id).toBe("00000000-0000-0000-0000-000000000000");
      expect(first.account?.identity.issuer).toBe("urn:jazz:system");
      expect(JSON.stringify(first.account)).not.toContain(backendSecret);
      const backend = first.client!;
      expect(backend.session?.user).toBe(first.account!.id);
      expect(backend.session?.identity).toEqual(first.account!.identity);
      const initial = await backend.db
        .insert(app.notes, { text: "service" })
        .wait({ tier: "global" });
      expect(
        await backend.db.one(app.notes.select("$createdBy").where({ id: initial.id })),
      ).toMatchObject({
        $createdBy: { account: backend.session?.user, identity: backend.session?.identity },
      });

      // Its core admits no guests: becoming one is refused, and the backend stays.
      await expect(session.createLocalFirst()).rejects.toThrow("not supported");
      expect(session.getSnapshot().client).toBe(backend);
      const user = await appUser(server, appId, backendSecret, "user");

      const current = backend;
      const scoped = backendDbForSession(current, user.session);
      await expect(async () => {
        await scoped.insert(app.notes, { text: "denied scoped user" }).wait({ tier: "global" });
      }).rejects.toThrow(/permission|denied|policy/i);
      const other = await appUser(server, appId, backendSecret, "other");
      const otherScope = backendDbForSession(current, other.session);
      const rows = await Promise.all([
        scoped.insert(app.posts, { text: "first scope" }).wait({ tier: "global" }),
        otherScope.insert(app.posts, { text: "second scope" }).wait({ tier: "global" }),
      ]);
      for (const [index, expected] of [user, other].entries()) {
        expect(
          await current.db.one(app.posts.select("$createdBy").where({ id: rows[index]!.id })),
        ).toMatchObject({ $createdBy: expected.author });
      }
      const attributed = backendDbAttributedTo(current, user.session);
      const row = await attributed
        .insert(app.notes, { text: "attributed service" })
        .wait({ tier: "global" });
      expect(
        await current.db.one(app.notes.select("$createdBy").where({ id: row.id })),
      ).toMatchObject({ $createdBy: user.author });
      expect(session.getSnapshot().account?.identity.issuer).toBe("urn:jazz:system");
      await session.close();
      expect(() => scoped.insert(app.notes, { text: "closed scope" })).toThrow(/closed|shut down/);
      expect(() => backendDbForSession(current, user.session)).toThrow(/closed|shut down/);
    } finally {
      await session.close();
      await server.stop();
    }
  }, 30_000);
  it("reads inside attributed transactions with the attributed Db's backend authority", async () => {
    const appId = randomUUID();
    const backendSecret = "attributed-tx-service-secret";
    const adminSecret = "attributed-tx-publication-secret";
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    let session: Awaited<ReturnType<typeof openJazzSession>> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });
      session = await openJazzSession({
        appId,
        serverUrl: server.url,
        app,
        permissions,
        driver: { type: "memory" },
        initial: { backendSecret },
      });
      const backend = session.getSnapshot().client!;
      const user = await appUser(server, appId, backendSecret, "user");
      const note = await backend.db.insert(app.notes, { text: "invite" }).wait({ tier: "global" });
      const attributed = backendDbAttributedTo(backend, user.session);

      const serializable = await attributed.serializableTransaction(async (tx) => {
        const read = await tx.one(app.notes.where({ id: note.id }));
        return { read, id: tx.insert(app.posts, { text: "serializable" }).id };
      });
      await serializable.wait();
      expect(serializable.value.read).toMatchObject({ text: "invite" });

      const mergeable = await attributed.transaction(async (tx) => {
        const read = await tx.one(app.notes.where({ id: note.id }));
        return { read, id: tx.insert(app.posts, { text: "mergeable" }).id };
      });
      await mergeable.wait({ tier: "global" });
      expect(mergeable.value.read).toMatchObject({ text: "invite" });

      // Users may not insert notes; the attributed transaction admits the
      // write as the backend while recording the user as its author.
      const admitted = await attributed.serializableTransaction(async (tx) => {
        await tx.one(app.notes.where({ id: note.id }));
        return tx.insert(app.notes, { text: "backend admitted" }).id;
      });
      await admitted.wait();
      expect(
        await backend.db.one(app.notes.select("$createdBy").where({ id: admitted.value })),
      ).toMatchObject({ $createdBy: user.author });

      await expect(
        attributed.serializableTransaction(async (tx) => {
          await tx.one(app.notes.where({ id: note.id }));
          tx.insert(app.posts, { text: "rolled back" });
          throw new Error("abort redeem");
        }),
      ).rejects.toThrow("abort redeem");
      expect(await backend.db.all(app.posts.where({ text: "rolled back" }))).toEqual([]);

      for (const id of [serializable.value.id, mergeable.value.id]) {
        expect(await backend.db.one(app.posts.select("$createdBy").where({ id }))).toMatchObject({
          $createdBy: user.author,
        });
      }
    } finally {
      await session?.close();
      await server.stop();
    }
  }, 30_000);
  it("reconnects and syncs queued writes after a sync-server outage longer than ten seconds", async () => {
    const appId = randomUUID();
    const backendSecret = "outage-service-secret";
    const dataDir = await mkdtemp(join(tmpdir(), "jazz-outage-"));
    let server = await startLocalJazzServer({ appId, backendSecret, dataDir });
    const open = () =>
      openJazzSession({
        appId,
        serverUrl: server.url,
        app,
        permissions,
        driver: { type: "memory" },
        initial: { backendSecret },
      });
    const writer = await open();
    let observer: Awaited<ReturnType<typeof open>> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret: server.adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });
      const db = writer.getSnapshot().client!.db;
      await db.insert(app.posts, { text: "before outage" }).wait({ tier: "global" });
      // A remote subscription opened before the outage keeps updating after it.
      const deliveries: string[][] = [];
      const subscriptionErrors: Error[] = [];
      const unsubscribe = db.subscribe(
        app.posts,
        {
          onUpdate: (rows) => deliveries.push(rows.map((row) => row.text).sort()),
          onError: (error) => subscriptionErrors.push(error),
        },
        { tier: "remote" },
      );
      await vi.waitFor(() => expect(deliveries.at(-1)).toEqual(["before outage"]));

      const port = server.port;
      await server.stop();
      const during = db.insert(app.posts, { text: "during outage" });
      await during.wait({ tier: "local" });
      const globalRead = db.all(app.posts, { tier: "remote" }).then(
        () => "resolved",
        (error: unknown) => error,
      );
      await new Promise((resolve) => setTimeout(resolve, 12_000));
      // The outage is reported to Global readers (a rejection, never an
      // uncaught error), while the session stays usable for local work.
      expect(await globalRead).toBeInstanceOf(Error);
      expect(writer.getSnapshot().status).toBe("ready");
      await db.insert(app.posts, { text: "late in outage" }).wait({ tier: "local" });

      server = await startLocalJazzServer({ appId, port, backendSecret, dataDir });
      // The client reconnects by itself within one capped backoff interval;
      // until then Global reads keep reporting the outage.
      await vi.waitFor(() => db.all(app.posts, { tier: "remote" }), {
        timeout: 15_000,
        interval: 250,
      });
      await db.insert(app.posts, { text: "after outage" }).wait({ tier: "global" });
      observer = await open();
      await vi.waitFor(
        async () => {
          const texts = (
            await observer!.getSnapshot().client!.db.all(app.posts, { tier: "remote" })
          )
            .map((row) => row.text)
            .sort();
          expect(texts).toEqual([
            "after outage",
            "before outage",
            "during outage",
            "late in outage",
          ]);
        },
        { timeout: 15_000, interval: 250 },
      );
      await observer
        .getSnapshot()
        .client!.db.insert(app.posts, { text: "from observer" })
        .wait({ tier: "global" });
      await vi.waitFor(
        () =>
          expect(deliveries.at(-1)).toEqual([
            "after outage",
            "before outage",
            "during outage",
            "from observer",
            "late in outage",
          ]),
        { timeout: 15_000, interval: 250 },
      );
      expect(subscriptionErrors).toEqual([]);
      unsubscribe();
    } finally {
      await observer?.close();
      await writer.close();
      await server.stop();
      await rm(dataDir, { recursive: true, force: true });
    }
  }, 60_000);
});
