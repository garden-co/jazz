import { createJazzContext, Db, type JazzContext } from "../backend/index.js";
import type { Session } from "../runtime/context.js";
import type { WasmSchema } from "../drivers/types.js";
import type { CompiledPermissions } from "../permissions/index.js";
import { deploy } from "../dev/catalogue.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "../dev/dev-server.js";

type PolicyTestAppSchema = { wasmSchema: WasmSchema };
export type PolicyTestAppOptions = {
  /** Override only the client credential; the local authority keeps its configured secret. */
  clientBackendSecret?: string | null;
};
type ExpectLike = (value: unknown) => {
  not: {
    toThrow(expected?: unknown): void;
  };
  toThrow(expected?: unknown): void;
  rejects: {
    toThrow(expected?: unknown): Promise<void>;
  };
};
type TestDbMethodCallback = (db: Db) => unknown;
type PendingWrite = {
  wait(options: { tier: "edge" }): Promise<unknown>;
};
type SeedWrite<T> = {
  readonly value: T;
  wait(options: { tier: "local" | "edge" }): Promise<T>;
};

/** @internal */
export async function settlePolicySeed<T>(write: SeedWrite<T>): Promise<T> {
  return write.wait({ tier: "local" });
}

/** @internal */
export async function settlePolicySeedForSessionReads<T>(write: SeedWrite<T>): Promise<T> {
  await settlePolicySeed(write);
  return write.wait({ tier: "edge" });
}

/**
 * Db used for testing permissions.
 * Supports all {@link Db} operations plus helpers for client-local write
 * staging and serving-authority rejection. A rejected write briefly exists as
 * an optimistic local batch, but is not persisted by the server.
 */
export type TestDb = Db & {
  /**
   * Assert that the callback does not throw while staging its write locally.
   * Write operations performed inside the callback are not persisted.
   */
  expectAllowed(callback: TestDbMethodCallback): void;

  /**
   * Assert that a write is rejected by the serving authority.
   *
   * Client writes are admitted optimistically, so this checks the write's edge
   * receipt rather than expecting synchronous local permission enforcement.
   */
  expectDenied(callback: (db: Db) => PendingWrite): Promise<void>;
};

function asTestDb(db: Db, expect: ExpectLike): TestDb {
  const testDb = db as TestDb;

  Object.defineProperties(testDb, {
    expectAllowed: {
      value: (callback: TestDbMethodCallback) => {
        const tx = db.beginTransaction();
        try {
          expect(() => callback(tx as unknown as Db)).not.toThrow();
        } finally {
          tx.rollback();
        }
      },
    },
    expectDenied: {
      value: async (callback: (db: Db) => PendingWrite) => {
        const write = callback(db);
        await expect(write.wait({ tier: "edge" })).rejects.toThrow(
          /AuthorizationDenied|Write rejected by server authorization/,
        );
      },
    },
  });

  return testDb;
}

/**
 * A test app for permissions tests. Simplifies setting up a test app and provides methods
 * for seeding the database and validating policy checks.
 */
export class PolicyTestApp {
  constructor(
    private readonly expect: ExpectLike,
    private readonly app: any,
    private readonly jazzContext: JazzContext,
    private readonly server: LocalJazzServerHandle,
  ) {}

  /**
   * Seed the database with one admin write and wait until the serving
   * authority has accepted it before returning. Session-scoped reads default
   * to the edge tier, so local staging alone can otherwise race their first
   * policy-evaluated query.
   */
  async seed<T>(callback: (db: Db) => SeedWrite<T>): Promise<T> {
    const db = this.jazzContext.asBackend();
    return settlePolicySeedForSessionReads(callback(db));
  }

  /**
   * Get a database client for the given session.
   */
  as(session: Session): TestDb {
    const db = this.jazzContext.forSession(session);
    return asTestDb(db, this.expect);
  }

  /**
   * Shutdown the test app. This will stop the local Jazz client and server.
   */
  async shutdown(): Promise<void> {
    await this.jazzContext.shutdown();
    await this.server.stop();
  }
}

/**
 * Create a new policy test app.
 * This will start a local Jazz server and push the schema catalogue to it.
 * @returns a {@link PolicyTestApp} instance that can be used to seed the database and validate policy checks.
 * @param app - The Jazz app created with `defineApp(...)`
 * @param permissions - The permissions created with `definePermissions(...)`
 * @param expectFn - The `expect` function to use for assertions (e.g. `expect` from `vitest`)
 */
export async function createPolicyTestApp(
  app: PolicyTestAppSchema,
  permissions: CompiledPermissions,
  expectFn: ExpectLike,
  options: PolicyTestAppOptions = {},
): Promise<PolicyTestApp> {
  const backendSecret = `backend-secret`;
  const adminSecret = `admin-secret`;
  const server = await startLocalJazzServer({
    backendSecret,
    adminSecret,
  });

  await deploy({
    appId: server.appId,
    serverUrl: server.url,
    adminSecret,
    schema: app,
    permissions,
  });

  const jazzContext = createJazzContext({
    appId: server.appId,
    app,
    permissions,
    driver: { type: "memory" },
    serverUrl: server.url,
    backendSecret:
      options.clientBackendSecret === undefined
        ? backendSecret
        : (options.clientBackendSecret ?? undefined),
    env: "test",
  });

  return new PolicyTestApp(expectFn, app, jazzContext, server);
}
