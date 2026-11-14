import { computeSchemaHash } from "../schema-hash.js";
import { fetchPublishedBundles } from "../dev/catalogue-api.js";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { anyOf, definePermissions } from "../permissions/index.js";
import { schema as s, migration as m } from "../index.js";
import { appMigrations } from "./client-migrations.js";
import {
  createPolicyTestApp,
  deploy,
  type LocalJazzServerHandle,
  startLocalJazzServer,
} from "./index.js";
import { LOCAL_FIRST_JWT_ISSUER } from "../runtime/client-session.js";
import { settlePolicySeed, settlePolicySeedForSessionReads } from "./policy-test-app.js";

const tempRoots: string[] = [];
const localServers = new Set<LocalJazzServerHandle>();
const testSchema = {
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      ownerId: s.uuid().optional(),
    },
    {},
  ),
};
type TestSchema = s.Schema<typeof testSchema>;
const testApp: s.App<TestSchema> = s.defineApp(testSchema, { migrations: "none" });
const testPermissions = definePermissions(testApp, ({ policy, session }) => {
  policy.todos.allowRead.where(anyOf([{ ownerId: session.user }, { ownerId: { isNull: true } }]));
  policy.todos.allowInsert.where({ ownerId: session.user });
});

afterEach(async () => {
  await Promise.all(
    Array.from(localServers, async (server) => {
      try {
        await server.stop();
      } finally {
        localServers.delete(server);
      }
    }),
  );

  await Promise.all(
    tempRoots.splice(0).map((rootPath) => rm(rootPath, { recursive: true, force: true })),
  );
});

async function createTempRoot(prefix: string): Promise<string> {
  const rootPath = await mkdtemp(join(tmpdir(), prefix));
  tempRoots.push(rootPath);
  return rootPath;
}

async function canBindPort(port: number): Promise<boolean> {
  return await new Promise<boolean>((resolve) => {
    const server = createServer();
    server.once("error", () => {
      resolve(false);
    });
    server.listen(port, "127.0.0.1", () => {
      server.close((error) => {
        void error;
        resolve(true);
      });
    });
  });
}

async function getAvailablePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }
          reject(new Error("Failed to allocate an available port."));
        });
        return;
      }

      const port = address.port;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
  });
}

async function startTrackedLocalJazzServer(
  options: Parameters<typeof startLocalJazzServer>[0],
): Promise<LocalJazzServerHandle> {
  const server = await startLocalJazzServer(options);
  localServers.add(server);
  return server;
}

async function stopTrackedLocalJazzServer(server: LocalJazzServerHandle): Promise<void> {
  try {
    await server.stop();
  } finally {
    localServers.delete(server);
  }
}

describe("startLocalJazzServer", () => {
  it("starts the process, waits for /health, and stops cleanly", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-capture-");
    const dataDir = join(captureRoot, "data-dir");
    const port = await getAvailablePort();

    const server = await startTrackedLocalJazzServer({
      appId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      port,
      dataDir,
      backendSecret: "test-backend-secret",
      adminSecret: "test-admin-secret",
    });

    try {
      const healthResponse = await fetch(`${server.url}/health`);
      expect(healthResponse.status).toBe(200);
      expect(server.adminSecret).toBe("test-admin-secret");
      expect(server.backendSecret).toBe("test-backend-secret");
    } finally {
      await stopTrackedLocalJazzServer(server);
    }
  }, 15_000);

  it("atomically assigns distinct ports to concurrent automatic servers", async () => {
    const firstRoot = await createTempRoot("jazz-tools-testing-auto-port-a-");
    const secondRoot = await createTempRoot("jazz-tools-testing-auto-port-b-");

    const [firstServer, secondServer] = await Promise.all([
      startTrackedLocalJazzServer({
        appId: "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        dataDir: join(firstRoot, "data-dir"),
      }),
      startTrackedLocalJazzServer({
        appId: "ffffffff-ffff-ffff-ffff-ffffffffffff",
        dataDir: join(secondRoot, "data-dir"),
      }),
    ]);

    try {
      expect(firstServer.port).not.toBe(secondServer.port);
      const healthResponses = await Promise.all([
        fetch(`${firstServer.url}/health`),
        fetch(`${secondServer.url}/health`),
      ]);
      expect(healthResponses.map((response) => response.status)).toEqual([200, 200]);
    } finally {
      await stopTrackedLocalJazzServer(firstServer);
      await stopTrackedLocalJazzServer(secondServer);
    }
  }, 20_000);

  it("frees the port after stop so it can be rebound", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-port-free-");
    const dataDir = join(captureRoot, "data-dir");
    const port = await getAvailablePort();

    const server = await startTrackedLocalJazzServer({
      appId: "cccccccc-cccc-cccc-cccc-cccccccccccc",
      port,
      dataDir,
    });

    await stopTrackedLocalJazzServer(server);

    const canRebind = await canBindPort(port);
    expect(canRebind).toBe(true);
  });

  it("can start a server with enableLogs turned on", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-logs-");
    const dataDir = join(captureRoot, "data-dir");
    const port = await getAvailablePort();

    const server = await startTrackedLocalJazzServer({
      appId: "dddddddd-dddd-dddd-dddd-dddddddddddd",
      port,
      dataDir,
      enableLogs: true,
    });

    try {
      const healthResponse = await fetch(`${server.url}/health`);
      expect(healthResponse.status).toBe(200);
    } finally {
      await stopTrackedLocalJazzServer(server);
    }
  }, 15_000);

  it("accepts a deployment via /admin/bundles when admin secret matches", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret-for-ts-schema-sync";

    const server = await startTrackedLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const response = await fetch(`${server.url}/apps/${server.appId}/admin/bundles`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": adminSecret,
        },
        body: JSON.stringify({
          targetSchemaHash: await computeSchemaHash(testApp.wasmSchema),
          schemas: [
            {
              hash: await computeSchemaHash(testApp.wasmSchema),
              schema: { tables: testApp.wasmSchema },
            },
          ],
          migrations: [],
          permissions: testPermissions,
        }),
      });

      expect(response.status).toBe(200);
    } finally {
      await stopTrackedLocalJazzServer(server);
    }
  });

  it("rejects a deployment via /admin/bundles when admin secret doesn't match", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret";

    const server = await startTrackedLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const response = await fetch(`${server.url}/apps/${server.appId}/admin/bundles`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": "wrong-admin-secret",
        },
        body: JSON.stringify({
          targetSchemaHash: await computeSchemaHash(testApp.wasmSchema),
          schemas: [
            {
              hash: await computeSchemaHash(testApp.wasmSchema),
              schema: { tables: testApp.wasmSchema },
            },
          ],
          migrations: [],
          permissions: testPermissions,
        }),
      });

      expect(response.status).toBe(401);
      // Listing the published bundles needs the same secret.
      await expect(
        fetchPublishedBundles(server.url, {
          appId: server.appId,
          adminSecret: "wrong-admin-secret",
        }),
      ).rejects.toThrow(/401/);
      await expect(
        fetchPublishedBundles(server.url, { appId: server.appId, adminSecret }),
      ).resolves.toEqual([]);
    } finally {
      await stopTrackedLocalJazzServer(server);
    }
  });
});

describe("deploy", () => {
  it("deploys the current schema object", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret";

    const server = await startTrackedLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const result = await deploy({
        serverUrl: server.url,
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret,
        schema: testApp,
        permissions: testPermissions,
      });

      expect(result.schema.hash).toBeTruthy();

      const response = await fetch(`${server.url}/apps/${server.appId}/schemas`, {
        headers: {
          "X-Jazz-Admin-Secret": adminSecret,
        },
      });
      expect(response.status).toBe(200);

      const body = (await response.json()) as { hashes?: string[] };
      expect(body.hashes?.length).toBeGreaterThan(0);
    } finally {
      await stopTrackedLocalJazzServer(server);
    }
  }, 30_000);

  it("rejects when server is unreachable", async () => {
    await expect(
      deploy({
        serverUrl: "http://127.0.0.1:9",
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret: "admin-secret",
        schema: testApp,
        permissions: testPermissions,
      }),
    ).rejects.toThrow();
  }, 10_000);
});

describe("createPolicyTestApp", () => {
  it("waits for local seed visibility and returns the settled value", async () => {
    let settle!: (value: { id: string }) => void;
    const settled = new Promise<{ id: string }>((resolve) => {
      settle = resolve;
    });
    const wait = vi.fn(() => settled);

    let resolved = false;
    const result = settlePolicySeed({ value: { id: "optimistic" }, wait }).then((value) => {
      resolved = true;
      return value;
    });
    await Promise.resolve();

    expect(wait).toHaveBeenCalledOnce();
    expect(wait).toHaveBeenCalledWith({ tier: "local" });
    expect(resolved).toBe(false);

    settle({ id: "settled" });
    await expect(result).resolves.toEqual({ id: "settled" });
  });

  it("waits for authority acceptance before a session-scoped seed is returned", async () => {
    const calls: Array<"local" | "global"> = [];
    const write = {
      value: { id: "seeded" },
      wait: vi.fn(async ({ tier }: { tier: "local" | "global" }) => {
        calls.push(tier);
        return { id: tier === "local" ? "locally-settled" : "authority-settled" };
      }),
    };

    await expect(settlePolicySeedForSessionReads(write)).resolves.toEqual({
      id: "authority-settled",
    });
    expect(calls).toEqual(["local", "global"]);
  });

  it("creates a test app from an app definition and compiled permissions", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, testPermissions, expect);

    try {
      const seeded = await policyTestApp.seed((db) => {
        return db.insert(testApp.todos, {
          title: "Ship the direct app API",
          done: false,
          ownerId: "00000000-0000-4000-8000-000000000001",
        });
      });

      const alice = policyTestApp.as({
        issuer: "https://policy-test.example",
        user_id: "alice",
        account_id: "00000000-0000-4000-8000-000000000001",
        claims: {},
        authMode: "external",
      });
      const bob = policyTestApp.as({
        issuer: "https://policy-test.example",
        user_id: "bob",
        account_id: "00000000-0000-4000-8000-000000000002",
        claims: {},
        authMode: "external",
      });

      await expect(alice.all(testApp.todos.where({ id: seeded.id }))).resolves.toEqual([
        expect.objectContaining({ id: seeded.id }),
      ]);
      await expect(bob.all(testApp.todos.where({ id: seeded.id }))).resolves.toEqual([]);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  // The /docs/tooling/testing pattern broke on an app's first migration: the
  // test server was handed only the current schema, so the app's client,
  // opening with the lenses the app ships, disagreed with it about the app's
  // first version (alpha.61 adopter round, E11).
  it("serves an app that ships a migration", async () => {
    const notesV1 = { notes: s.table({ title: s.string(), owner_id: s.uuid() }, {}) };
    const notesV2 = {
      notes: s.table({ title: s.string(), owner_id: s.uuid(), body: s.string().optional() }, {}),
    };
    const migration = m.defineMigration({
      fromHash: await computeSchemaHash(s.defineApp(notesV1, { migrations: "none" }).wasmSchema),
      toHash: await computeSchemaHash(s.defineApp(notesV2, { migrations: "none" }).wasmSchema),
      from: notesV1,
      to: notesV2,
      migrate: { notes: { body: m.add.string({ default: null }) } },
    });
    const app = s.defineApp(notesV2, {
      migrations: await appMigrations(notesV1, { to: notesV2, migration }),
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.notes.allowRead.where({ owner_id: session.user });
      policy.notes.allowInsert.where({ owner_id: session.user });
    });
    const policyTestApp = await createPolicyTestApp(app, permissions, expect);

    try {
      const alice = policyTestApp.as({
        issuer: "https://policy-test.example",
        user_id: "alice",
        account_id: "00000000-0000-4000-8000-000000000001",
        claims: {},
        authMode: "external",
      });
      const note = await alice
        .insert(app.notes, {
          title: "first",
          owner_id: "00000000-0000-4000-8000-000000000001",
          body: "hello",
        })
        .wait({ tier: "global" });

      await expect(alice.all(app.notes, { tier: "remote" })).resolves.toEqual([
        expect.objectContaining({ id: note.id, title: "first", body: "hello" }),
      ]);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  // tobiaslins's reproduction from #4771: a local-first session on an app
  // whose bundle ships two schema versions (a table added in between). It
  // timed out waiting for query coverage until createPolicyTestApp
  // published the app's history as createJazz does.
  it("answers a local-first session on an app that ships two schema versions", async () => {
    const before = { notes: s.table({ text: s.string() }, {}) };
    const after = { ...before, tags: s.table({ label: s.string() }, {}) };
    const migration = m.defineMigration({
      from: before,
      to: after,
      createTables: { tags: true },
    });
    const app = s.defineApp(after, {
      migrations: await appMigrations(before, { to: after, migration }),
    });
    const permissions = definePermissions(app, ({ policy }) => {
      policy.notes.allowRead.always();
      policy.tags.allowRead.always();
    });
    const policyTestApp = await createPolicyTestApp(app, permissions, expect);

    try {
      const alice = policyTestApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });
      await expect(alice.all(app.notes)).resolves.toEqual([]);
      await expect(alice.all(app.tags)).resolves.toEqual([]);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  it("rejects a seed callback that returns the inserted row instead of the write", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, testPermissions, expect);

    try {
      await expect(
        policyTestApp.seed(
          // @ts-expect-error: returning `.value` is the mistake the runtime check catches.
          (db) => db.insert(testApp.todos, { title: "unwrapped", done: false }).value,
        ),
      ).rejects.toThrow(/must return the write result itself/);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  it("limits backend SYSTEM bootstrap to the configured authority credential", async () => {
    const noCredential = await createPolicyTestApp(testApp, testPermissions, expect, {
      clientBackendSecret: null,
    });
    try {
      await expect(
        noCredential.seed((db) =>
          db.insert(testApp.todos, { title: "no credential", done: false }),
        ),
      ).rejects.toThrow(/backendSecret required/);
    } finally {
      await noCredential.shutdown();
    }

    const wrongCredential = await createPolicyTestApp(testApp, testPermissions, expect, {
      clientBackendSecret: "wrong-backend-secret",
    });
    try {
      await expect(
        wrongCredential.seed((db) =>
          db.insert(testApp.todos, { title: "wrong credential", done: false }),
        ),
      ).rejects.toThrow(/authorization|credential|backend|rejected/i);
    } finally {
      await wrongCredential.shutdown();
    }
  }, 10_000);

  it("runs policies as a local-first guest session without an explicit issuer", async () => {
    const guestSchema = {
      products: s.table({ title: s.string() }, {}),
      staffNotes: s.table({ body: s.string() }, {}),
      carts: s.table({ item: s.string(), ownerId: s.uuid() }, {}),
    };
    const guestApp: s.App<s.Schema<typeof guestSchema>> = s.defineApp(guestSchema, {
      migrations: "none",
    });
    const guestPermissions = definePermissions(guestApp, ({ policy, session, allOf }) => {
      policy.products.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.products.allowInsert.where(session.where({ authMode: "external" }));
      policy.staffNotes.allowRead.where(session.where({ authMode: "external" }));
      policy.carts.allowRead.where({ ownerId: session.user });
      policy.carts.allowInsert.where(
        allOf([{ ownerId: session.user }, session.where({ authMode: "local-first" })]),
      );
    });
    const policyTestApp = await createPolicyTestApp(guestApp, guestPermissions, expect);

    try {
      const product = await policyTestApp.seed((db) =>
        db.insert(guestApp.products, { title: "Visible to guests" }),
      );
      const note = await policyTestApp.seed((db) =>
        db.insert(guestApp.staffNotes, { body: "Staff only" }),
      );

      const guestSession = {
        user_id: "guest-device",
        claims: {},
        authMode: "local-first",
      } as const;
      const guest = policyTestApp.as(guestSession);
      const guestAccount = policyTestApp.accountFor(guestSession);
      const otherAccount = policyTestApp.accountFor({ ...guestSession, user_id: "other-device" });
      expect(otherAccount).not.toBe(guestAccount);

      await expect(guest.all(guestApp.products.where({ id: product.id }))).resolves.toEqual([
        expect.objectContaining({ id: product.id }),
      ]);
      await expect(guest.all(guestApp.staffNotes.where({ id: note.id }))).resolves.toEqual([]);

      const cart = await guest
        .insert(guestApp.carts, { item: "Tea", ownerId: guestAccount })
        .wait({ tier: "global" });
      await expect(guest.all(guestApp.carts.where({ id: cart.id }))).resolves.toEqual([
        expect.objectContaining({ id: cart.id, ownerId: guestAccount }),
      ]);

      await guest.expectDenied((db) => db.insert(guestApp.products, { title: "Guest product" }));
      await guest.expectDenied((db) =>
        db.insert(guestApp.carts, { item: "Someone else's", ownerId: otherAccount }),
      );
    } finally {
      await policyTestApp.shutdown();
    }
  }, 20_000);

  it("keeps an explicit external issuer as a trusted forSession actor", async () => {
    const actorSchema = {
      carts: s.table({ item: s.string(), ownerId: s.uuid() }, {}),
    };
    const actorApp: s.App<s.Schema<typeof actorSchema>> = s.defineApp(actorSchema, {
      migrations: "none",
    });
    const actorPermissions = definePermissions(actorApp, ({ policy, session }) => {
      policy.carts.allowRead.where({ ownerId: session.user });
      policy.carts.allowInsert.where({ ownerId: session.user });
    });
    const policyTestApp = await createPolicyTestApp(actorApp, actorPermissions, expect);

    try {
      // The pre-existing forSession semantics: the session is the external
      // issuer's principal acting for the given account. Only an omitted or
      // reserved issuer makes a self-signed local-first guest.
      const account = "00000000-0000-4000-8000-000000000002";
      const actorSession = {
        issuer: "https://identity.policy-test.example",
        user_id: "member",
        account_id: account,
        claims: {},
        authMode: "local-first",
      } as const;
      expect(policyTestApp.accountFor(actorSession)).toBe(account);

      const actor = policyTestApp.as(actorSession);
      const cart = await actor
        .insert(actorApp.carts, { item: "Tea", ownerId: account })
        .wait({ tier: "global" });
      await expect(actor.all(actorApp.carts.where({ id: cart.id }))).resolves.toEqual([
        expect.objectContaining({ id: cart.id, ownerId: account }),
      ]);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 20_000);

  it("rejects an explicit account for a self-signed local-first guest", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, testPermissions, expect);

    try {
      expect(() =>
        policyTestApp.as({
          user_id: "guest-device",
          account_id: "00000000-0000-4000-8000-000000000002",
          claims: {},
          authMode: "local-first",
        }),
      ).toThrow(/founding account derived from their key/);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  it("treats the reserved local-first issuer as a self-signed guest", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, testPermissions, expect);

    try {
      const withIssuer = policyTestApp.sessionFor({
        issuer: LOCAL_FIRST_JWT_ISSUER,
        user_id: "guest-device",
        claims: {},
        authMode: "local-first",
      });
      const withoutIssuer = policyTestApp.sessionFor({
        user_id: "guest-device",
        claims: {},
        authMode: "local-first",
      });
      expect(withIssuer).toMatchObject({
        issuer: LOCAL_FIRST_JWT_ISSUER,
        authMode: "local-first",
        user_id: withoutIssuer.user_id,
        account_id: withoutIssuer.account_id,
      });
      expect(withIssuer.user_id).not.toBe("guest-device");

      expect(() =>
        policyTestApp.as({
          issuer: LOCAL_FIRST_JWT_ISSUER,
          user_id: "guest-device",
          account_id: "00000000-0000-4000-8000-000000000002",
          claims: {},
          authMode: "local-first",
        }),
      ).toThrow(/founding account derived from their key/);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  it("rejects custom claims for a self-signed local-first guest", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, testPermissions, expect);

    try {
      expect(() =>
        policyTestApp.as({
          user_id: "guest-device",
          claims: { join_code: "invite-123" },
          authMode: "local-first",
        }),
      ).toThrow(/local-first guests cannot carry custom claims/);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  it("admits concurrent upserts of one row from the same session in order", async () => {
    // Every session write installs that session's claims before it is
    // admitted. The first upsert stays suspended on the owner, so the second
    // write's claims must queue behind it instead of re-entering it.
    const upsertPermissions = definePermissions(testApp, ({ policy, session }) => {
      policy.todos.allowRead.where({ ownerId: session.user });
      policy.todos.allowInsert.where({ ownerId: session.user });
      policy.todos.allowUpdate.where({ ownerId: session.user });
    });
    const policyTestApp = await createPolicyTestApp(testApp, upsertPermissions, expect);

    try {
      const ownerId = "00000000-0000-4000-8000-000000000001";
      const alice = policyTestApp.as({
        issuer: "https://policy-test.example",
        user_id: "alice",
        account_id: ownerId,
        claims: {},
        authMode: "external",
      });
      const id = "00000000-0000-4000-8000-0000000000a1";

      await Promise.all([
        alice.upsert(testApp.todos, id, { title: "first", done: false, ownerId }).wait({
          tier: "global",
        }),
        alice.upsert(testApp.todos, id, { title: "second", done: false, ownerId }).wait({
          tier: "global",
        }),
      ]);

      await expect(alice.all(testApp.todos.where({ id }))).resolves.toEqual([
        expect.objectContaining({ id, title: "second" }),
      ]);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  it("exposes expectAllowed and expectDenied on session-scoped test dbs", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, testPermissions, expect);

    try {
      const alice = policyTestApp.as({
        issuer: "https://policy-test.example",
        user_id: "alice",
        account_id: "00000000-0000-4000-8000-000000000001",
        claims: {},
        authMode: "external",
      });
      const bob = policyTestApp.as({
        issuer: "https://policy-test.example",
        user_id: "bob",
        account_id: "00000000-0000-4000-8000-000000000002",
        claims: {},
        authMode: "external",
      });

      alice.expectAllowed((db) => {
        db.insert(testApp.todos, {
          title: "Alice can insert her own todo",
          done: false,
          ownerId: "00000000-0000-4000-8000-000000000001",
        });
      });

      await bob.expectDenied((db) => {
        return db.insert(testApp.todos, {
          title: "Bob cannot insert Alice's todo",
          done: false,
          ownerId: "00000000-0000-4000-8000-000000000001",
        });
      });

      await expect(alice.all(testApp.todos)).resolves.toEqual([]);
      await expect(bob.all(testApp.todos)).resolves.toEqual([]);
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);
});

describe("expectDenied", () => {
  const owned = definePermissions(testApp, ({ policy, session }) => {
    policy.todos.allowRead.where({ ownerId: session.user });
    policy.todos.allowUpdate.where({ ownerId: session.user });
  });
  const session = (user_id: string, account_id: string) =>
    ({
      issuer: "https://policy-test.example",
      user_id,
      account_id,
      claims: {},
      authMode: "external",
    }) as const;

  it("counts an update the server denies, because the user may not read the row", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, owned, expect);
    try {
      const todo = await policyTestApp.seed((db) =>
        db.insert(testApp.todos, {
          title: "Alice's",
          done: false,
          ownerId: "00000000-0000-4000-8000-000000000001",
        }),
      );
      const bob = policyTestApp.as(session("bob", "00000000-0000-4000-8000-000000000002"));
      // The replica stages it, and the server's refusal counts.
      await bob.expectDenied((db) => db.update(testApp.todos, todo.id, { title: "Bob's now" }));
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  it("counts an update denied at once, of a row deleted on this replica", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, owned, expect);
    try {
      const todo = await policyTestApp.seed((db) =>
        db.insert(testApp.todos, {
          title: "Alice's",
          done: false,
          ownerId: "00000000-0000-4000-8000-000000000001",
        }),
      );
      await policyTestApp.seed((db) => db.delete(testApp.todos, todo.id));
      const bob = policyTestApp.as(session("bob", "00000000-0000-4000-8000-000000000002"));
      // The local denial carries the code a server rejection carries.
      let thrown: unknown;
      try {
        bob.update(testApp.todos, todo.id, { title: "Bob's now" });
      } catch (error) {
        thrown = error;
      }
      expect((thrown as { code?: string } | undefined)?.code).toBe("permission_denied");
      await bob.expectDenied((db) => db.update(testApp.todos, todo.id, { title: "Bob's now" }));
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);

  it("fails, saying so, when the write is accepted", async () => {
    const policyTestApp = await createPolicyTestApp(testApp, owned, expect);
    try {
      const alice = policyTestApp.as(session("alice", "00000000-0000-4000-8000-000000000001"));
      const todo = await policyTestApp.seed((db) =>
        db.insert(testApp.todos, {
          title: "Alice's",
          done: false,
          ownerId: "00000000-0000-4000-8000-000000000001",
        }),
      );
      await expect(
        alice.expectDenied((db) => db.update(testApp.todos, todo.id, { title: "still hers" })),
      ).rejects.toThrow("expected the write to be denied, but it was accepted");
    } finally {
      await policyTestApp.shutdown();
    }
  }, 10_000);
});

describe("policies that correlate optional and required columns", () => {
  // `exists.where` equalities between an optional column and a required column
  // of the same type used to be rejected when the server converted the policy
  // (OperandTypeMismatch), although the TypeScript DSL accepted them.
  const inviteSchema = {
    shows: s.table({ name: s.string() }, {}),
    invites: s.table(
      { showId: s.uuid(), code: s.string(), parentId: s.uuid().optional() },
      { show: s.rel("shows", "showId"), parent: s.rel("shows", "parentId") },
    ),
    members: s.table(
      {
        showId: s.uuid(),
        parentId: s.uuid(),
        account: s.uuid(),
        inviteCode: s.string().optional(),
      },
      { show: s.rel("shows", "showId"), parent: s.rel("shows", "parentId") },
    ),
  };
  type InviteSchema = s.Schema<typeof inviteSchema>;
  const inviteApp: s.App<InviteSchema> = s.defineApp(inviteSchema, { migrations: "none" });
  const invitePermissions = definePermissions(inviteApp, ({ policy, session }) => {
    policy.shows.allowRead.always();
    policy.members.allowRead.where({ account: session.user });
    policy.members.allowInsert.where((member) =>
      policy.invites.exists.where({
        showId: member.showId,
        // Optional text compared with required text.
        code: member.inviteCode,
        // Required reference compared with an optional reference.
        parentId: member.parentId,
      }),
    );
  });

  it("publishes the policy and enforces the correlation", async () => {
    const policyTestApp = await createPolicyTestApp(inviteApp, invitePermissions, expect);
    try {
      const show = await policyTestApp.seed((db) => db.insert(inviteApp.shows, { name: "Gig" }));
      await policyTestApp.seed((db) =>
        db.insert(inviteApp.invites, { showId: show.id, code: "current", parentId: show.id }),
      );
      const account = "00000000-0000-4000-8000-000000000003";
      const carol = policyTestApp.as({
        issuer: "https://policy-test.example",
        user_id: "carol",
        account_id: account,
        claims: {},
        authMode: "external",
      });

      await carol.expectDenied((db) =>
        db.insert(inviteApp.members, {
          showId: show.id,
          parentId: show.id,
          account,
          inviteCode: "guessed",
        }),
      );
      await carol.expectDenied((db) =>
        db.insert(inviteApp.members, { showId: show.id, parentId: show.id, account }),
      );
      // An invite whose optional parentId is NULL must not match a member whose
      // parentId is set, even though every other correlated column matches.
      await policyTestApp.seed((db) =>
        db.insert(inviteApp.invites, { showId: show.id, code: "orphan", parentId: null }),
      );
      await carol.expectDenied((db) =>
        db.insert(inviteApp.members, {
          showId: show.id,
          parentId: show.id,
          account,
          inviteCode: "orphan",
        }),
      );
      await carol
        .insert(inviteApp.members, {
          showId: show.id,
          parentId: show.id,
          account,
          inviteCode: "current",
        })
        .wait({ tier: "global" });
    } finally {
      await policyTestApp.shutdown();
    }
  }, 20_000);
});

describe("startLocalJazzServer with an app that ships a migration", () => {
  // Started with an app's schema, the test server used to take the app's
  // current version as its first: the app's own createJazz then failed to
  // publish its migration chain ("deployment cannot replace the runtime
  // genesis schema"), and clients opening with its lenses refused queries.
  it("serves the app's createJazz and its migration chain", async () => {
    const notesV1 = { notes: s.table({ title: s.string() }, {}) };
    const notesV2 = { notes: s.table({ title: s.string(), body: s.string().optional() }, {}) };
    const v1Hash = await computeSchemaHash(s.defineApp(notesV1, { migrations: "none" }).wasmSchema);
    const migration = m.defineMigration({
      fromHash: v1Hash,
      toHash: await computeSchemaHash(s.defineApp(notesV2, { migrations: "none" }).wasmSchema),
      from: notesV1,
      to: notesV2,
      migrate: { notes: { body: m.add.string({ default: null }) } },
    });
    const app = s.defineApp(notesV2, {
      migrations: await appMigrations(notesV1, { to: notesV2, migration }),
    });
    const permissions = definePermissions(app, ({ policy }) => {
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
    });
    const server = await startLocalJazzServer({ inMemory: true, schema: app, permissions });
    const { createJazz, remote } = await import("../backend/index.js");
    const jazz = createJazz({
      app,
      permissions,
      store: remote({ url: server.url, appId: server.appId, secret: server.adminSecret }),
    });
    try {
      await jazz.ready();
      const note = await jazz.db
        .insert(app.notes, { title: "first", body: "hello" })
        .wait({ tier: "global" });
      await expect(jazz.db.all(app.notes, { tier: "remote" })).resolves.toEqual([
        expect.objectContaining({ id: note.id, title: "first", body: "hello" }),
      ]);
    } finally {
      await jazz.close();
      await server.stop();
    }
  }, 20_000);

  it("refuses a trusted workload for such an app instead of starting a server that would refuse it", async () => {
    const v1 = { notes: s.table({ title: s.string() }, {}) };
    const v2 = { notes: s.table({ title: s.string(), body: s.string().optional() }, {}) };
    const migration = m.defineMigration({
      fromHash: await computeSchemaHash(s.defineApp(v1, { migrations: "none" }).wasmSchema),
      toHash: await computeSchemaHash(s.defineApp(v2, { migrations: "none" }).wasmSchema),
      from: v1,
      to: v2,
      migrate: { notes: { body: m.add.string({ default: null }) } },
    });
    const app = s.defineApp(v2, { migrations: await appMigrations(v1, { to: v2, migration }) });
    await expect(
      startLocalJazzServer({
        inMemory: true,
        schema: app,
        permissions: {},
        trustedWorkload: { issuer: "https://workload.example", audience: "jazz", subject: "ci" },
      }),
    ).rejects.toThrow(/ships migrations is not supported with trustedWorkload yet/);
  });
});
