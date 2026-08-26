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
import { setTrustedReservedSession } from "./db-internal-session.js";
import { canonicalAuthorSubject } from "./author-id.js";

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
      session: { user: canonicalAuthorSubject(LOCAL_FIRST_JWT_ISSUER, "alice") },
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
        user: canonicalAuthorSubject("https://issuer.example", "alice"),
        claims: expect.objectContaining({ role: "reader" }),
      },
    });
  });

  it("reports backend-scoped auth state for session-backed dbs", () => {
    const session = {
      user: canonicalAuthorSubject("https://issuer.example", "alice"),
      claims: { role: "writer", iss: "https://issuer.example", sub: "alice" },
      authMode: "external" as const,
    };
    const runtimeClient = {
      updateAuthToken: vi.fn(),
      onMutationError: vi.fn(),
    };

    const db = new TestDb(
      {
        appId: "test-app",
        jwtToken: makeJwt({ sub: "bob", claims: { role: "reader" } }),
      },
      runtimeClient as any,
      { authMode: session.authMode, session },
    );

    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session,
    });

    db.updateAuthToken(makeJwt({ sub: "bob", claims: { role: "admin" } }));

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
        jwtToken: makeJwt({ sub: "alice", claims: { role: "reader" } }),
      },
      runtimeClient as any,
    );
    const scopedDb = new TestDb(
      {
        appId: "test-app",
        jwtToken: makeJwt({ sub: "alice", claims: { role: "reader" } }),
      },
      runtimeClient as any,
      {
        authMode: "external",
        session: {
          user: canonicalAuthorSubject("https://issuer.example", "bob"),
          claims: { role: "writer", iss: "https://issuer.example", sub: "bob" },
          authMode: "external",
        },
      },
    );

    scopedDb.updateAuthToken(makeJwt({ sub: "bob", claims: { role: "admin" } }));

    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(sharedDb.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: canonicalAuthorSubject("https://issuer.example", "alice"),
      },
    });
    expect(scopedDb.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: canonicalAuthorSubject("https://issuer.example", "bob"),
      },
    });
  });

  it("returns the initial bearer auth state", () => {
    const { db } = makeDbWithJwt(makeJwt({ sub: "alice", claims: { role: "reader" } }));

    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: canonicalAuthorSubject("https://issuer.example", "alice"),
        claims: expect.objectContaining({ role: "reader" }),
      },
    });
    expect(db.getAuthState().error).toBeUndefined();
  });

  it("updates auth for same-principal JWT refresh", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice" }));
    const refreshed = makeJwt({ sub: "alice", claims: { role: "writer" } });
    const states: AuthState[] = [];

    const stop = db.onAuthChanged((state) => {
      states.push(state);
    });
    db.touchClient();

    db.updateAuthToken(refreshed);
    stop();

    expect(runtimeClient.updateAuthToken).toHaveBeenCalledWith(refreshed);
    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: canonicalAuthorSubject("https://issuer.example", "alice"),
        claims: expect.objectContaining({ role: "writer" }),
      },
    });
    expect(db.getAuthState().error).toBeUndefined();
    expect(states.at(-1)).toMatchObject({
      authMode: "external",
    });
    expect(states.at(-1)?.error).toBeUndefined();
  });

  it("ignores redundant auth updates when the token is unchanged", () => {
    const jwt = makeJwt({ sub: "alice", claims: { role: "reader" } });
    const { db, runtimeClient } = makeDbWithJwt(jwt);
    const states: AuthState[] = [];

    const stop = db.onAuthChanged((state) => {
      states.push(state);
    });

    db.updateAuthToken(jwt);
    stop();

    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(states).toHaveLength(1);
    expect(states[0]).toMatchObject({
      authMode: "external",
      session: {
        user: canonicalAuthorSubject("https://issuer.example", "alice"),
      },
    });
    expect(states[0]?.error).toBeUndefined();
  });

  it("rejects logout principal changes on a live db", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice" }));

    expect(() => db.updateAuthToken(null)).toThrow(
      "Changing auth principal on a live client is not supported. Recreate the Db.",
    );
    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: canonicalAuthorSubject("https://issuer.example", "alice"),
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

    const stop = db.onAuthChanged((state) => {
      states.push(state);
    });
    db.touchClient();

    db.updateCookieSession(refreshed);
    stop();

    expect(runtimeClient.updateCookieSession).toHaveBeenCalledWith(refreshed);
    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: canonicalAuthorSubject("https://issuer.example", "alice"),
        claims: expect.objectContaining({ role: "writer" }),
      },
    });
    expect(states.at(-1)).toMatchObject({
      authMode: "external",
    });
  });
});
