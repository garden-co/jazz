import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { anyOf, definePermissions } from "../permissions/index.js";
import { schema as s } from "../index.js";
import {
  createPolicyTestApp,
  deploy,
  type LocalJazzServerHandle,
  startLocalJazzServer,
} from "./index.js";
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
const testApp: s.App<TestSchema> = s.defineApp(testSchema);
const testPermissions = definePermissions(testApp, ({ policy, session }) => {
  policy.todos.allowRead.where(
    anyOf([{ ownerId: session.user.account }, { ownerId: { isNull: true } }]),
  );
  policy.todos.allowInsert.where({ ownerId: session.user.account });
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

  it("accepts a schema publish via /admin/schemas when admin secret matches", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret-for-ts-schema-sync";

    const server = await startTrackedLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const response = await fetch(`${server.url}/apps/${server.appId}/admin/schemas`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": adminSecret,
        },
        body: JSON.stringify({ schema: { tables: testApp.wasmSchema } }),
      });

      expect(response.status).toBe(201);
    } finally {
      await stopTrackedLocalJazzServer(server);
    }
  });

  it("rejects a schema publish via /admin/schemas when admin secret doesn't match", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret";

    const server = await startTrackedLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const response = await fetch(`${server.url}/apps/${server.appId}/admin/schemas`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": "wrong-admin-secret",
        },
        body: JSON.stringify({ schema: { tables: testApp.wasmSchema } }),
      });

      expect(response.status).toBe(401);
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
    const guestApp: s.App<s.Schema<typeof guestSchema>> = s.defineApp(guestSchema);
    const guestPermissions = definePermissions(guestApp, ({ policy, session, allOf }) => {
      policy.products.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.products.allowInsert.where(session.where({ authMode: "external" }));
      policy.staffNotes.allowRead.where(session.where({ authMode: "external" }));
      policy.carts.allowRead.where({ ownerId: session.user.account });
      policy.carts.allowInsert.where(
        allOf([{ ownerId: session.user.account }, session.where({ authMode: "local-first" })]),
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
    const actorApp: s.App<s.Schema<typeof actorSchema>> = s.defineApp(actorSchema);
    const actorPermissions = definePermissions(actorApp, ({ policy, session }) => {
      policy.carts.allowRead.where({ ownerId: session.user.account });
      policy.carts.allowInsert.where({ ownerId: session.user.account });
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
