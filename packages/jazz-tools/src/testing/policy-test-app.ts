import { createJazzContext, Db, Session, type JazzContext } from "../backend/index.js";
import type { WasmSchema } from "../drivers/types.js";
import { DbTransactionScope } from "../index.js";
import type { CompiledPermissions } from "../permissions/index.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../runtime/schema-fetch.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./local-jazz-server.js";

type PolicyTestAppSchema = { wasmSchema: WasmSchema };
type ExpectLike = (value: unknown) => {
  not: {
    toThrow(expected?: unknown): void;
  };
  toThrow(expected?: unknown): void;
};
type TestDbMethodCallback = (db: DbTransactionScope) => unknown;
type TestDbFactory = () => Db;

/**
 * Db used for testing permissions.
 * Supports all {@link Db} operations plus the {@link TestDb.expectAllowed} and {@link TestDb.expectDenied}
 * helpers to test write operations without producing side effects on the test database.
 */
export type TestDb = Db & {
  /**
   * Assert that the callback does not throw a policy error.
   * Write operations performed inside the callback are not persisted.
   */
  expectAllowed(callback: TestDbMethodCallback): void;

  /**
   * Assert that the callback throws a policy error.
   * Write operations performed inside the callback are not persisted.
   */
  expectDenied(callback: TestDbMethodCallback): void;
};

function asTestDb(db: Db, expect: ExpectLike, createProbeDb: TestDbFactory): TestDb {
  const testDb = db as TestDb;

  Object.defineProperties(testDb, {
    expectAllowed: {
      value: (callback: TestDbMethodCallback) => {
        const tx = createProbeDb().beginTransaction();
        expect(() => callback(tx)).not.toThrow();
        tx.rollback();
      },
    },
    expectDenied: {
      value: (callback: TestDbMethodCallback) => {
        const tx = createProbeDb().beginTransaction();
        expect(() => callback(tx)).toThrow('WriteError("policy denied');
        tx.rollback();
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
  private readonly probeContexts: JazzContext[] = [];

  constructor(
    private readonly expect: ExpectLike,
    private readonly app: any,
    private readonly permissions: CompiledPermissions,
    private readonly jazzContext: JazzContext,
    private readonly server: LocalJazzServerHandle,
    private readonly backendSecret: string,
  ) {}

  /**
   * Seed the database with the given callback.
   * The callback is executed in an admin database context.
   */
  seed<T>(callback: (db: Db) => T): T {
    const db = this.jazzContext.asBackend(this.app);
    return callback(db);
  }

  /**
   * Get a database client for the given session.
   */
  as(session: Session): TestDb {
    const db = this.jazzContext.forSession(session, this.app);
    return asTestDb(db, this.expect, () => this.createProbeDb(session));
  }

  /**
   * Shutdown the test app. This will stop the local Jazz client and server.
   */
  async shutdown(): Promise<void> {
    await Promise.all(this.probeContexts.splice(0).map((context) => context.shutdown()));
    await this.jazzContext.shutdown();
    await this.server.stop();
  }

  private createProbeDb(session: Session): Db {
    const context = createJazzContext({
      appId: this.server.appId,
      app: this.app,
      permissions: this.permissions,
      driver: { type: "memory" },
      serverUrl: this.server.url,
      backendSecret: this.backendSecret,
      env: "test",
      userBranch: "main",
    });
    this.probeContexts.push(context);
    return context.forSession(session, this.app);
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
): Promise<PolicyTestApp> {
  const backendSecret = `backend-secret`;
  const adminSecret = `admin-secret`;
  const server = await startLocalJazzServer({
    backendSecret,
    adminSecret,
  });

  const { hash: schemaHash } = await publishStoredSchema(server.url, {
    appId: server.appId,
    adminSecret,
    schema: app.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(server.url, {
    appId: server.appId,
    adminSecret,
  });
  await publishStoredPermissions(server.url, {
    appId: server.appId,
    adminSecret,
    schemaHash,
    permissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });

  const jazzContext = createJazzContext({
    appId: server.appId,
    app,
    permissions,
    driver: { type: "memory" },
    serverUrl: server.url,
    backendSecret,
    env: "test",
    userBranch: "main",
  });

  return new PolicyTestApp(expectFn, app, permissions, jazzContext, server, backendSecret);
}
