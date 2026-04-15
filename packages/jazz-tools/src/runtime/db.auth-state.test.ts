import { describe, expect, it, vi } from "vitest";
import { createDbFromClient } from "./db.js";
import type { AuthState } from "./auth-state.js";

function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.signature`;
}

function makeDbWithJwt(jwtToken: string) {
  const runtimeClient = {
    updateAuthToken: vi.fn(),
  };

  const db = createDbFromClient(
    {
      appId: "test-app",
      jwtToken,
    },
    runtimeClient as any,
  );

  return { db, runtimeClient };
}

describe("Db auth state", () => {
  it("reports backend-scoped auth state for session-backed dbs", () => {
    const session = {
      user_id: "alice",
      claims: { role: "writer" },
    };
    const runtimeClient = {
      updateAuthToken: vi.fn(),
    };

    const db = createDbFromClient(
      {
        appId: "test-app",
        jwtToken: makeJwt({ sub: "bob", claims: { role: "reader" } }),
      },
      runtimeClient as any,
      session,
      "alice@writer",
    );

    expect(db.getAuthState()).toMatchObject({
      status: "authenticated",
      transport: "backend",
      session,
    });

    db.updateAuthToken(makeJwt({ sub: "bob", claims: { role: "admin" } }));

    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(db.getAuthState()).toMatchObject({
      status: "authenticated",
      transport: "backend",
      session,
    });
  });

  it("does not leak scoped auth updates into a shared runtime client", () => {
    const runtimeClient = {
      updateAuthToken: vi.fn(),
    };

    const sharedDb = createDbFromClient(
      {
        appId: "test-app",
        jwtToken: makeJwt({ sub: "alice", claims: { role: "reader" } }),
      },
      runtimeClient as any,
    );
    const scopedDb = createDbFromClient(
      {
        appId: "test-app",
        jwtToken: makeJwt({ sub: "alice", claims: { role: "reader" } }),
      },
      runtimeClient as any,
      { user_id: "bob", claims: { role: "writer" } },
      "bob@writer",
    );

    scopedDb.updateAuthToken(makeJwt({ sub: "bob", claims: { role: "admin" } }));

    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(sharedDb.getAuthState()).toMatchObject({
      status: "authenticated",
      transport: "bearer",
      session: {
        user_id: "alice",
      },
    });
    expect(scopedDb.getAuthState()).toMatchObject({
      status: "authenticated",
      transport: "backend",
      session: {
        user_id: "bob",
      },
    });
  });

  it("returns the initial bearer auth state", () => {
    const { db } = makeDbWithJwt(makeJwt({ sub: "alice", claims: { role: "reader" } }));

    expect(db.getAuthState()).toMatchObject({
      status: "authenticated",
      transport: "bearer",
      session: {
        user_id: "alice",
        claims: expect.objectContaining({ role: "reader" }),
      },
    });
  });

  it("updates auth for same-principal JWT refresh", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice" }));
    const refreshed = makeJwt({ sub: "alice", claims: { role: "writer" } });
    const states: AuthState[] = [];

    const stop = db.onAuthChanged((state) => {
      states.push(state);
    });

    db.updateAuthToken(refreshed);
    stop();

    expect(runtimeClient.updateAuthToken).toHaveBeenCalledWith(refreshed);
    expect(db.getAuthState()).toMatchObject({
      status: "authenticated",
      transport: "bearer",
      session: {
        user_id: "alice",
        claims: expect.objectContaining({ role: "writer" }),
      },
    });
    expect(states.at(-1)).toMatchObject({
      status: "authenticated",
      transport: "bearer",
    });
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
      status: "authenticated",
      transport: "bearer",
      session: {
        user_id: "alice",
      },
    });
  });

  it("rejects logout principal changes on a live db", () => {
    const { db, runtimeClient } = makeDbWithJwt(makeJwt({ sub: "alice" }));

    expect(() => db.updateAuthToken(null)).toThrow(
      "Changing auth principal on a live client is not supported. Recreate the Db.",
    );
    expect(runtimeClient.updateAuthToken).not.toHaveBeenCalled();
    expect(db.getAuthState()).toMatchObject({
      status: "authenticated",
      transport: "bearer",
      session: {
        user_id: "alice",
      },
    });
  });
});
