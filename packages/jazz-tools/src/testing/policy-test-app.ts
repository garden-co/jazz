import { createJazzContext, type JazzContext } from "../backend/create-jazz-context.js";
import { Db } from "../runtime/db.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import { createHash } from "node:crypto";
import { mintLocalFirstToken, verifyLocalFirstIdentityProof } from "jazz-napi";
import { localFirstSessionFromToken } from "../backend/request-auth.js";
import { ANONYMOUS_JWT_ISSUER, LOCAL_FIRST_JWT_ISSUER } from "../runtime/client-session.js";
import type { AuthMode, Session } from "../runtime/context.js";
import type { WasmSchema } from "../drivers/types.js";
import type { CompiledPermissions } from "../permissions/index.js";
import { deploy } from "../dev/catalogue.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "../dev/dev-server.js";

type PolicyTestAppSchema = { wasmSchema: WasmSchema };
export type PolicyTestAppOptions = {
  /** Override only the client credential; the local authority keeps its configured secret. */
  clientBackendSecret?: string | null;
};
/**
 * Session accepted by {@link PolicyTestApp.as}.
 *
 * Local-first sessions always use Jazz's reserved local-first issuer at
 * runtime, so `issuer` is omitted for `authMode: "local-first"`. Their
 * `user_id` names the guest device: the test app derives a local-first key
 * from it and acts with that key's verified subject and founding account,
 * exactly like a real local-first client (see {@link PolicyTestApp.accountFor}).
 */
export type PolicyTestSession =
  | (Omit<Session, "issuer" | "authMode"> & {
      authMode: Exclude<AuthMode, "local-first">;
      issuer: string;
    })
  | (Omit<Session, "issuer" | "authMode"> & {
      authMode: "local-first";
      issuer?: typeof LOCAL_FIRST_JWT_ISSUER;
    });
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
  wait(options: { tier: "global" }): Promise<unknown>;
};
type SeedWrite<T> = {
  readonly value: T;
  wait(options: { tier: "local" | "global" }): Promise<T>;
};

/** @internal */
export async function settlePolicySeed<T>(write: SeedWrite<T>): Promise<T> {
  if (typeof (write as { wait?: unknown } | null | undefined)?.wait !== "function") {
    throw new TypeError(
      "PolicyTestApp.seed: the callback must return the write result itself " +
        "(for example `(db) => db.insert(table, data)`), not its `.value`.",
    );
  }
  return write.wait({ tier: "local" });
}

/** @internal */
export async function settlePolicySeedForSessionReads<T>(write: SeedWrite<T>): Promise<T> {
  await settlePolicySeed(write);
  return write.wait({ tier: "global" });
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
   * Client writes are admitted optimistically, so this checks Core's write
   * outcome rather than expecting synchronous local permission enforcement.
   */
  expectDenied(callback: (db: Db) => PendingWrite): Promise<void>;
};

/**
 * `forSession` is a trusted test-only backend entry point. Non-anonymous
 * policy actors receive a stable synthetic account so rejected writes reach
 * the policy gate instead of the durable-author precondition.
 */
function policyTestAccountId(session: Session): string {
  return localFirstAccountId(
    "jazz-runtime-test-account-fixtures",
    JSON.stringify([session.issuer, session.user_id]),
  );
}

// Self-signed local-first proofs are accepted for at most one hour.
const LOCAL_FIRST_TOKEN_TTL_SECONDS = 3600;

/**
 * Build the session a real local-first client presents to a backend: a
 * self-signed local-first token for a key derived from `user_id`, verified and
 * bound to the registry's founding account exactly as `forRequest()` does.
 * The native runtime then admits it through the same proof check, so no
 * reserved-issuer check is bypassed.
 */
function localFirstPolicySession(
  appId: string,
  session: Extract<PolicyTestSession, { authMode: "local-first" }>,
): Session {
  if (session.issuer !== undefined && session.issuer !== LOCAL_FIRST_JWT_ISSUER) {
    throw new Error(
      `PolicyTestApp.as: local-first sessions use issuer "${LOCAL_FIRST_JWT_ISSUER}"; ` +
        `omit \`issuer\` instead of passing "${session.issuer}".`,
    );
  }
  if (session.account_id !== undefined) {
    throw new Error(
      "PolicyTestApp.as: local-first sessions own the founding account derived from their key; " +
        "omit `account_id` and read it from `testApp.accountFor(session)` instead.",
    );
  }
  const seed = createHash("sha256")
    .update(`jazz-policy-test-local-first\0${session.user_id}`)
    .digest("base64url");
  const token = mintLocalFirstToken(seed, appId, LOCAL_FIRST_TOKEN_TTL_SECONDS);
  const admitted = localFirstSessionFromToken(token, appId, verifyLocalFirstIdentityProof);
  admitted.claims = { ...admitted.claims, ...session.claims };
  return admitted;
}

function withPolicyTestAccount(session: Session): Session {
  if (
    session.account_id !== undefined ||
    session.authMode === "anonymous" ||
    session.issuer === ANONYMOUS_JWT_ISSUER
  )
    return session;
  return {
    ...session,
    account_id: policyTestAccountId(session),
  };
}

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
        await expect(write.wait({ tier: "global" })).rejects.toThrow(
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
   * to remote/Core confirmation, so local staging alone can otherwise race their first
   * policy-evaluated query.
   */
  async seed<T>(callback: (db: Db) => SeedWrite<T>): Promise<T> {
    const db = this.jazzContext.asBackend();
    return settlePolicySeedForSessionReads(callback(db));
  }

  /**
   * Get a database client for the given session.
   */
  as(session: PolicyTestSession): TestDb {
    const db = this.jazzContext.forSession(this.policySession(session));
    return asTestDb(db, this.expect);
  }

  /**
   * The account a session acts for. For a local-first session this is the
   * founding account derived from its key, as a real local-first client gets.
   */
  accountFor(session: PolicyTestSession): string {
    return this.policySession(session).account_id!;
  }

  private policySession(session: PolicyTestSession): Session {
    if (session.authMode !== "local-first") return withPolicyTestAccount(session as Session);
    return localFirstPolicySession(this.server.appId, session);
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
