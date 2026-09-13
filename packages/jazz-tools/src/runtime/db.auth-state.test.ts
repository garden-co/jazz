import { describe, expect, it, vi } from "vitest";
import { Db, type DbConfig } from "./db.js";
import type { AuthState } from "./auth-state.js";
import type { Session } from "./context.js";
import type { JazzClient } from "./client.js";
import { RuntimeSource, type RuntimeClientContext } from "./runtime-source.js";
import {
  internalSessionFromVerifiedReservedJwtPayload,
  LOCAL_FIRST_JWT_ISSUER,
} from "./client-session.js";
import { getDbInternalSession, setTrustedReservedSession } from "./db-internal-session.js";
function author(issuer: string, subject: string) {
  return { account: null, identity: { issuer, subject } };
}

function withTrustedSession(config: DbConfig, session: Session): DbConfig {
  setTrustedReservedSession(config, session);
  return config;
}

class TestRuntimeSource extends RuntimeSource<DbConfig> {
  constructor(private readonly client: JazzClient) {
    super();
  }

  override createClient(_context: RuntimeClientContext<DbConfig>): JazzClient {
    return this.client;
  }
}

class TestDb extends Db {
  constructor(
    config: DbConfig,
    private readonly client: JazzClient,
    scopedAuthState?: AuthState,
  ) {
    super(
      config,
      new TestRuntimeSource(client),
      scopedAuthState
        ? {
            initialState: scopedAuthState,
            lockAuthenticatedState: true,
          }
        : undefined,
    );
  }

  touchClient(): void {
    this.getClient({ auth_state_touch: { columns: [] } });
  }
}

function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url({ iss: "https://issuer.example", ...payload })}.signature`;
}

function makeDbWithJwt(jwtToken: string) {
  const runtimeClient = {
    updateAuthToken: vi.fn(),
    updateCookieSession: vi.fn(),
    onMutationError: vi.fn(),
  };
  const db = new TestDb(
    {
      appId: "test-app",
      jwtToken,
    },
    runtimeClient as any,
  );

  return { db, runtimeClient };
}

function makeCookieSession(version: string, user_id = "alice"): Session {
  return {
    user_id,
    claims: {
      version,
      auth_mode: "external",
      subject: user_id,
      issuer: "https://issuer.example",
    },
    issuer: "https://issuer.example",
    authMode: "external",
  };
}

function jwtClaimVersion(jwtToken: string | undefined): unknown {
  if (!jwtToken) return undefined;
  const payload = jwtToken.split(".")[1];
  return JSON.parse(Buffer.from(payload!, "base64url").toString("utf8")).version;
}

interface AuthTestRuntimeClient {
  updateAuthToken: { mock: { calls: unknown[][] } };
  updateCookieSession: { mock: { calls: unknown[][] } };
}

function transportSnapshot(db: TestDb, runtimeClient: AuthTestRuntimeClient) {
  const config = db.getConfig();
  const hasJwt = config.jwtToken !== undefined;
  const hasCookie = config.cookieSession !== undefined;
  return {
    mode: hasJwt ? "bearer" : "cookie",
    exclusive: { hasJwt, hasCookie },
    claimVersion: hasJwt ? jwtClaimVersion(config.jwtToken) : config.cookieSession?.claims.version,
    forwarded: {
      bearer: runtimeClient.updateAuthToken.mock.calls.length,
      cookie: runtimeClient.updateCookieSession.mock.calls.length,
    },
  };
}
interface AuthPublicationSnapshot {
  publicState: {
    authMode: AuthState["authMode"];
    version: unknown;
  };
  config: {
    bearerVersion: unknown;
    cookieVersion: unknown;
  };
  internal: {
    authMode: Session["authMode"] | undefined;
    userId: string | undefined;
    version: unknown;
  };
  transport: {
    bearerVersion: unknown;
    cookieVersion: unknown;
  };
}

function authPublicationSnapshot(
  db: TestDb,
  runtimeClient: AuthTestRuntimeClient,
  state: AuthState,
): AuthPublicationSnapshot {
  const config = db.getConfig();
  const internal = getDbInternalSession(db);
  const lastBearer = runtimeClient.updateAuthToken.mock.calls.at(-1)?.[0] as string | undefined;
  const lastCookie = runtimeClient.updateCookieSession.mock.calls.at(-1)?.[0] as
    | Session
    | undefined;
  return {
    publicState: {
      authMode: state.authMode,
      version: state.session?.claims.version,
    },
    config: {
      bearerVersion: jwtClaimVersion(config.jwtToken),
      cookieVersion: config.cookieSession?.claims.version,
    },
    internal: {
      authMode: internal?.authMode,
      userId: internal?.user_id,
      version: internal?.claims.version,
    },
    transport: {
      bearerVersion: jwtClaimVersion(lastBearer),
      cookieVersion: lastCookie?.claims.version,
    },
  };
}

function makeDbWithCookieSession(cookieSession: Session) {
  const runtimeClient = {
    updateAuthToken: vi.fn(),
    updateCookieSession: vi.fn(),
    onMutationError: vi.fn(),
  };
  const db = new TestDb(
    {
      appId: "cookie-auth-app",
      cookieSession,
    },
    runtimeClient as any,
  );

  return { db, runtimeClient };
}

describe("Db auth state", () => {
  it("keeps transport identity out of Db properties and ignores planted aliases", () => {
    const { db } = makeDbWithJwt(makeJwt({ sub: "alice", role: "reader" }));
    const forbidden = new Set(["issuer", "user_id", "internalSession", "trustedReservedSession"]);

    expect(
      Reflect.ownKeys(db).filter((key) => typeof key === "string" && forbidden.has(key)),
    ).toEqual([]);
    expect(getDbInternalSession(db)).toMatchObject({
      issuer: "https://issuer.example",
      user_id: "alice",
    });

    Object.assign(db as object, {
      issuer: "https://attacker.example",
      user_id: "mallory",
      internalSession: {
        issuer: "https://attacker.example",
        user_id: "mallory",
        claims: { role: "admin" },
        authMode: "external",
      },
      trustedReservedSession: null,
    });

    expect(getDbInternalSession(db)).toMatchObject({
      issuer: "https://issuer.example",
      user_id: "alice",
      claims: { role: "reader" },
    });
    expect(db.getAuthState().session).toMatchObject({
      user: author("https://issuer.example", "alice"),
      claims: expect.objectContaining({ role: "reader" }),
    });
  });

  it("refreshes a dedicated local-first session without entering generic JWT admission", () => {
    const initialToken = makeJwt({ iss: LOCAL_FIRST_JWT_ISSUER, sub: "alice", version: 1 });
    const initialSession = internalSessionFromVerifiedReservedJwtPayload(
      { iss: LOCAL_FIRST_JWT_ISSUER, sub: "alice" },
      "local-first",
    )!;
    const refreshedToken = makeJwt({
      iss: LOCAL_FIRST_JWT_ISSUER,
      sub: "alice",
      version: 2,
    });
    const runtimeClient = {
      updateTrustedAuthToken: vi.fn(),
      onMutationError: vi.fn(),
    };
    const runtimeSource = new (class extends TestRuntimeSource {
      override mintLocalFirstToken = vi.fn(() => refreshedToken);
    })(runtimeClient as any);
    const db = new (class extends Db {
      constructor() {
        super(
          withTrustedSession(
            {
              appId: "test-app",
              jwtToken: initialToken,
            },
            initialSession,
          ),
          runtimeSource,
        );
      }

      refreshForTest(): void {
        this.initLocalFirstAuth("alice-secret", 3600, false);
        (this as unknown as { refreshLocalFirstToken(): void }).refreshLocalFirstToken();
      }

      touchClient(): void {
        this.getClient({ auth_state_touch: { columns: [] } });
      }
    })();
    db.touchClient();

    db.refreshForTest();

    expect(db.getAuthState()).toMatchObject({
      authMode: "local-first",
      session: { user: author(LOCAL_FIRST_JWT_ISSUER, "alice") },
    });
    expect(runtimeClient.updateTrustedAuthToken).toHaveBeenCalledWith(
      refreshedToken,
      expect.objectContaining({ issuer: LOCAL_FIRST_JWT_ISSUER, user_id: "alice" }),
    );
  });

  it("returns the initial cookie auth state", () => {
    const { db } = makeDbWithCookieSession({
      user_id: "alice",
      claims: {
        role: "reader",
        auth_mode: "external",
        subject: "alice-subject",
        issuer: "https://issuer.example",
      },
      issuer: "https://issuer.example",
      authMode: "external",
    });

    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: author("https://issuer.example", "alice"),
        claims: expect.objectContaining({ role: "reader" }),
      },
    });
  });

  it("reports backend-scoped auth state for session-backed dbs", () => {
    const session = {
      user: author("https://issuer.example", "alice"),
      claims: { role: "writer" },
      authMode: "external" as const,
    };
    const runtimeClient = {
      updateAuthToken: vi.fn(),
      onMutationError: vi.fn(),
    };

    const db = new TestDb(
      {
        appId: "test-app",
        jwtToken: makeJwt({ sub: "bob", role: "reader" }),
      },
      runtimeClient as any,
      { authMode: session.authMode, session },
    );

    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session,
    });

    db.updateAuthToken(makeJwt({ sub: "bob", role: "admin" }));

    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session,
    });
  });

  it("does not leak scoped auth updates into a shared runtime client", () => {
    const runtimeClient = {
      updateAuthToken: vi.fn(),
      onMutationError: vi.fn(),
    };

    const sharedDb = new TestDb(
      {
        appId: "test-app",
        jwtToken: makeJwt({ sub: "alice", role: "reader" }),
      },
      runtimeClient as any,
    );
    const scopedDb = new TestDb(
      {
        appId: "test-app",
        jwtToken: makeJwt({ sub: "alice", role: "reader" }),
      },
      runtimeClient as any,
      {
        authMode: "external",
        session: {
          user: author("https://issuer.example", "bob"),
          claims: { role: "writer" },
          authMode: "external",
        },
      },
    );

    scopedDb.updateAuthToken(makeJwt({ sub: "bob", role: "admin" }));

    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(sharedDb.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: author("https://issuer.example", "alice"),
      },
    });
    expect(scopedDb.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: author("https://issuer.example", "bob"),
      },
    });
  });

  it("returns the initial bearer auth state", () => {
    const { db } = makeDbWithJwt(makeJwt({ sub: "alice", role: "reader" }));

    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: author("https://issuer.example", "alice"),
        claims: expect.objectContaining({ role: "reader" }),
      },
    });
    expect(db.getAuthState().error).toBeUndefined();
  });

  it("updates auth for same-principal JWT refresh", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice" }));
    const refreshed = makeJwt({ sub: "alice", role: "writer" });
    const states: AuthState[] = [];
    const listenerInternalRoles: unknown[] = [];

    const stop = db.onAuthChanged((state) => {
      states.push(state);
      listenerInternalRoles.push(getDbInternalSession(db)?.claims.role);
    });
    db.touchClient();

    db.updateAuthToken(refreshed);
    stop();

    expect(runtimeClient.updateAuthToken).toHaveBeenCalledWith(refreshed);
    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: author("https://issuer.example", "alice"),
        claims: expect.objectContaining({ role: "writer" }),
      },
    });
    expect(db.getAuthState().error).toBeUndefined();
    expect(states.at(-1)).toMatchObject({
      authMode: "external",
    });
    expect(states.at(-1)?.error).toBeUndefined();
    expect(listenerInternalRoles.at(-1)).toBe("writer");
  });

  it("ignores redundant auth updates when the token is unchanged", () => {
    const jwt = makeJwt({ sub: "alice", role: "reader" });
    const { db, runtimeClient } = makeDbWithJwt(jwt);
    const states: AuthState[] = [];
    const before = getDbInternalSession(db);

    const stop = db.onAuthChanged((state) => {
      states.push(state);
    });

    db.updateAuthToken(jwt);
    stop();

    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(getDbInternalSession(db)).toBe(before);
    expect(states).toHaveLength(1);
    expect(states[0]).toMatchObject({
      authMode: "external",
      session: {
        user: author("https://issuer.example", "alice"),
      },
    });
    expect(states[0]?.error).toBeUndefined();
  });

  it("rejects logout principal changes on a live db", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice" }));
    const before = getDbInternalSession(db);

    expect(() => db.updateAuthToken(null)).toThrow(
      "Changing auth principal on a live client is not supported. Recreate the Db.",
    );
    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(getDbInternalSession(db)).toBe(before);
    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: author("https://issuer.example", "alice"),
      },
    });
    expect(db.getAuthState().error).toBeUndefined();
  });

  it("updates mirrored cookie auth for the same principal", () => {
    const { db, runtimeClient } = makeDbWithCookieSession({
      user_id: "alice",
      claims: {
        role: "reader",
        auth_mode: "external",
        subject: "alice-subject",
        issuer: "https://issuer.example",
      },
      issuer: "https://issuer.example",
      authMode: "external",
    });
    const refreshed: Session = {
      user_id: "alice",
      claims: {
        role: "writer",
        auth_mode: "external",
        subject: "alice-subject",
        issuer: "https://issuer.example",
      },
      issuer: "https://issuer.example",
      authMode: "external",
    };
    const states: AuthState[] = [];
    const listenerInternalRoles: unknown[] = [];

    const stop = db.onAuthChanged((state) => {
      states.push(state);
      listenerInternalRoles.push(getDbInternalSession(db)?.claims.role);
    });
    db.touchClient();

    db.updateCookieSession(refreshed);
    stop();

    expect(runtimeClient.updateCookieSession).toHaveBeenCalledWith(refreshed);
    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: author("https://issuer.example", "alice"),
        claims: expect.objectContaining({ role: "writer" }),
      },
    });
    expect(states.at(-1)).toMatchObject({
      authMode: "external",
    });
    expect(listenerInternalRoles.at(-1)).toBe("writer");

    const beforeNoOp = getDbInternalSession(db);
    db.updateCookieSession({ ...refreshed, claims: { ...refreshed.claims } });
    expect(getDbInternalSession(db)).toBe(beforeNoOp);
    expect(runtimeClient.updateCookieSession).toHaveBeenCalledTimes(1);

    const accepted = getDbInternalSession(db);
    expect(() =>
      db.updateCookieSession({
        ...refreshed,
        user_id: "bob",
      }),
    ).toThrow("Changing auth principal on a live client is not supported. Recreate the Db.");
    expect(getDbInternalSession(db)).toBe(accepted);
    expect(runtimeClient.updateCookieSession).toHaveBeenCalledTimes(1);
  });

  it("keeps bearer and cookie transitions mode-exclusive across public state and transport", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice", version: "A" }));
    const observedStates: Array<{
      authMode: AuthState["authMode"];
      subject: string | undefined;
      version: unknown;
    }> = [];
    const stop = db.onAuthChanged((state) => {
      observedStates.push({
        authMode: state.authMode,
        subject: state.session?.user.identity.subject,
        version: state.session?.claims.version,
      });
    });
    db.touchClient();

    const snapshots = [transportSnapshot(db, runtimeClient)];
    db.updateCookieSession(makeCookieSession("B"));
    snapshots.push(transportSnapshot(db, runtimeClient));
    db.updateAuthToken(makeJwt({ sub: "alice", version: "C" }));
    snapshots.push(transportSnapshot(db, runtimeClient));
    db.updateCookieSession(makeCookieSession("D"));
    snapshots.push(transportSnapshot(db, runtimeClient));
    stop();

    expect(observedStates).toEqual([
      { authMode: "external", subject: "alice", version: "A" },
      { authMode: "external", subject: "alice", version: "B" },
      { authMode: "external", subject: "alice", version: "C" },
      { authMode: "external", subject: "alice", version: "D" },
    ]);
    expect(snapshots).toEqual([
      {
        mode: "bearer",
        exclusive: { hasJwt: true, hasCookie: false },
        claimVersion: "A",
        forwarded: { bearer: 0, cookie: 0 },
      },
      {
        mode: "cookie",
        exclusive: { hasJwt: false, hasCookie: true },
        claimVersion: "B",
        forwarded: { bearer: 0, cookie: 1 },
      },
      {
        mode: "bearer",
        exclusive: { hasJwt: true, hasCookie: false },
        claimVersion: "C",
        forwarded: { bearer: 1, cookie: 1 },
      },
      {
        mode: "cookie",
        exclusive: { hasJwt: false, hasCookie: true },
        claimVersion: "D",
        forwarded: { bearer: 1, cookie: 2 },
      },
    ]);
  });

  it("notifies auth observers after committing one coherent mode-exclusive snapshot", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice", version: "A" }));
    const snapshots: AuthPublicationSnapshot[] = [];
    const stop = db.onAuthChanged((state) => {
      snapshots.push(authPublicationSnapshot(db, runtimeClient, state));
    });
    snapshots.length = 0;
    db.touchClient();

    db.updateCookieSession(makeCookieSession("B"));
    stop();

    expect(snapshots).toEqual([
      {
        publicState: { authMode: "external", version: "B" },
        config: { bearerVersion: undefined, cookieVersion: "B" },
        internal: { authMode: "external", userId: "alice", version: "B" },
        transport: { bearerVersion: undefined, cookieVersion: "B" },
      },
    ]);
  });

  it("keeps a committed snapshot when an auth observer throws", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice", version: "A" }));
    db.touchClient();
    const observedVersions: unknown[] = [];
    const observerError = new Error("synthetic auth observer failure");
    let throwForVersionB = false;
    const stop = db.onAuthChanged((state) => {
      const version = state.session?.claims.version;
      observedVersions.push(version);
      if (throwForVersionB && version === "B") throw observerError;
    });
    throwForVersionB = true;

    expect(() => db.updateCookieSession(makeCookieSession("B"))).toThrow(observerError);
    throwForVersionB = false;

    expect(authPublicationSnapshot(db, runtimeClient, db.getAuthState())).toEqual({
      publicState: { authMode: "external", version: "B" },
      config: { bearerVersion: undefined, cookieVersion: "B" },
      internal: { authMode: "external", userId: "alice", version: "B" },
      transport: { bearerVersion: undefined, cookieVersion: "B" },
    });

    db.updateCookieSession(makeCookieSession("C"));
    stop();

    expect(observedVersions).toEqual(["A", "B", "C"]);
    expect(db.getAuthState().session?.claims.version).toBe("C");
  });

  it("rejects stale principals and live clears without changing the accepted snapshot", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice", version: "A" }));
    db.touchClient();
    db.updateCookieSession(makeCookieSession("B"));

    const acceptedState = db.getAuthState();
    const acceptedTransport = transportSnapshot(db, runtimeClient);
    const acceptedInternalSession = getDbInternalSession(db);
    const observedStates: AuthState[] = [];
    const stop = db.onAuthChanged((state) => observedStates.push(state));
    const bearerUpdates = runtimeClient.updateAuthToken.mock.calls.length;
    const cookieUpdates = runtimeClient.updateCookieSession.mock.calls.length;

    const rejection = "Changing auth principal on a live client is not supported. Recreate the Db.";
    expect(() => db.updateCookieSession(makeCookieSession("stale-cookie", "bob"))).toThrow(
      rejection,
    );
    expect(() => db.updateAuthToken(makeJwt({ sub: "bob", version: "stale-bearer" }))).toThrow(
      rejection,
    );
    expect(() => db.updateCookieSession(null)).toThrow(rejection);
    expect(() => db.updateAuthToken(null)).toThrow(rejection);
    stop();

    expect(db.getAuthState()).toBe(acceptedState);
    expect(transportSnapshot(db, runtimeClient)).toEqual(acceptedTransport);
    expect(getDbInternalSession(db)).toBe(acceptedInternalSession);
    expect(runtimeClient.updateAuthToken.mock.calls).toHaveLength(bearerUpdates);
    expect(runtimeClient.updateCookieSession.mock.calls).toHaveLength(cookieUpdates);
    expect(observedStates).toHaveLength(1);
  });

  it("rolls back a synchronous transport propagation failure before accepting a later update", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice", version: "A" }));
    db.touchClient();
    const beforeState = db.getAuthState();
    const beforeTransport = transportSnapshot(db, runtimeClient);
    const beforeInternalSession = getDbInternalSession(db);
    const observedVersions: unknown[] = [];
    const stop = db.onAuthChanged((state) => {
      observedVersions.push(state.session?.claims.version);
    });
    const propagationFailure = new Error("synthetic transport propagation failure");
    runtimeClient.updateCookieSession.mockImplementationOnce(() => {
      throw propagationFailure;
    });

    expect(() => db.updateCookieSession(makeCookieSession("B"))).toThrow(propagationFailure);
    expect(db.getAuthState()).toBe(beforeState);
    expect(transportSnapshot(db, runtimeClient)).toMatchObject({
      mode: beforeTransport.mode,
      exclusive: beforeTransport.exclusive,
      claimVersion: beforeTransport.claimVersion,
    });
    expect(getDbInternalSession(db)).toBe(beforeInternalSession);
    expect(observedVersions).toEqual(["A"]);

    db.updateCookieSession(makeCookieSession("C"));
    stop();

    expect(db.getAuthState().session?.claims.version).toBe("C");
    expect(transportSnapshot(db, runtimeClient)).toEqual({
      mode: "cookie",
      exclusive: { hasJwt: false, hasCookie: true },
      claimVersion: "C",
      forwarded: { bearer: 0, cookie: 2 },
    });
    expect(observedVersions).toEqual(["A", "C"]);
  });
});
