import { describe, expect, it } from "vitest";
import { canonicalAuthorSubject } from "./author-id.js";
import { createAuthStateStore, type AuthState } from "./auth-state.js";

function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.signature`;
}

const validJwt = makeJwt({ sub: "alice", iss: "https://issuer.example" });

describe("auth-state", () => {
  it("keeps the last session while unauthenticated", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: makeJwt({
        iss: "https://issuer.example",
        sub: "alice",
        claims: { role: "reader" },
      }),
    });

    store.markUnauthenticated("expired");

    expect(store.getState()).toEqual({
      authMode: "external",
      error: "expired",
      session: {
        user: canonicalAuthorSubject("https://issuer.example", "alice"),
        claims: { iss: "https://issuer.example", sub: "alice", role: "reader" },
        authMode: "external",
      },
    });
  });

  it("deduplicates repeated auth-loss notifications", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice" }),
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
      error: "expired",
    });
  });

  it("rejects principal hot-swap on a live client", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice" }),
    });

    expect(() =>
      store.applyJwtToken(makeJwt({ iss: "https://issuer.example", sub: "bob" })),
    ).toThrow("Changing auth principal on a live client is not supported. Recreate the Db.");
  });

  it("treats the issuer as part of the live principal identity", () => {
    const store = createAuthStateStore({
      appId: "test-app",
      jwtToken: makeJwt({ iss: "https://issuer-a.example", sub: "alice" }),
    });

    expect(() =>
      store.applyJwtToken(makeJwt({ iss: "https://issuer-b.example", sub: "alice" })),
    ).toThrow("Changing auth principal on a live client is not supported. Recreate the Db.");
  });
});

describe("AuthState — flattened shape", () => {
  it("authenticated state has authMode + session, no error", () => {
    const store = createAuthStateStore({ appId: "a", jwtToken: validJwt });
    const state = store.getState();
    expect(state.session?.authMode).toBe("external");
    expect(state.error).toBeUndefined();
    // @ts-expect-error — status has been removed
    state.status;
    // @ts-expect-error — transport has been removed
    state.transport;
  });

  it("markUnauthenticated sets error but preserves last-known session", () => {
    const store = createAuthStateStore({ appId: "a", jwtToken: validJwt });
    const before = store.getState().session;
    store.markUnauthenticated("expired");
    const after = store.getState();
    expect(after.error).toBe("expired");
    expect(after.session).toEqual(before);
  });

  it("applyJwtToken clears error on success", () => {
    const store = createAuthStateStore({ appId: "a", jwtToken: validJwt });
    store.markUnauthenticated("expired");
    store.applyJwtToken(validJwt);
    expect(store.getState().error).toBeUndefined();
  });

  it("exposes external authMode from generic JWTs at construction", () => {
    const store = createAuthStateStore({ appId: "a", jwtToken: validJwt });
    expect(store.getState().authMode).toBe("external");
  });
});
