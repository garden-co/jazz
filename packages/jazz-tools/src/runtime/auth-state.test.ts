import { describe, expect, it } from "vitest";
import { createAuthStateStore, type AuthState } from "./auth-state.js";

function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.signature`;
}

describe("auth-state", () => {
  it("keeps the last session while unauthenticated", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: makeJwt({ sub: "alice", claims: { role: "reader" } }),
    });

    store.markUnauthenticated("expired");

    expect(store.getState()).toEqual({
      status: "unauthenticated",
      reason: "expired",
      session: {
        user_id: "alice",
        claims: {
          role: "reader",
          auth_mode: "external",
          subject: "alice",
        },
      },
    });
  });

  it("deduplicates repeated auth-loss notifications", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: makeJwt({ sub: "alice" }),
    });
    const states: AuthState[] = [];

    const stop = store.onChange((state) => {
      states.push(state);
    });

    store.markUnauthenticated("expired");
    store.markUnauthenticated("expired");
    stop();

    expect(states).toHaveLength(2);
    expect(states[1]).toMatchObject({
      status: "unauthenticated",
      reason: "expired",
    });
  });

  it("rejects principal hot-swap on a live client", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: makeJwt({ sub: "alice" }),
    });

    expect(() => store.applyJwtToken(makeJwt({ sub: "bob" }))).toThrow(
      "Changing auth principal on a live client is not supported. Recreate the Db.",
    );
  });

  it("rejects anonymous-to-jwt swap on a live client", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      localAuthMode: "anonymous",
      localAuthToken: "device-token",
    });

    expect(() => store.applyJwtToken(makeJwt({ sub: "alice" }))).toThrow(
      "Changing auth principal on a live client is not supported. Recreate the Db.",
    );
  });

  it("rejects logout principal change on a live client", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: makeJwt({ sub: "alice" }),
      localAuthMode: "anonymous",
      localAuthToken: "device-token",
    });

    expect(() => store.applyJwtToken(undefined)).toThrow(
      "Changing auth principal on a live client is not supported. Recreate the Db.",
    );
  });

  it("falls back to local auth when jwt cannot be parsed", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: "not-a-jwt",
      localAuthMode: "demo",
      localAuthToken: "device-token",
    });

    expect(store.getState()).toMatchObject({
      status: "authenticated",
      transport: "local",
      session: {
        claims: {
          auth_mode: "local",
          local_mode: "demo",
        },
      },
    });
  });
});
