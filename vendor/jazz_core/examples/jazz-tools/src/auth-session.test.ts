import assert from "node:assert/strict";
import { verify } from "node:crypto";
import test from "node:test";
import {
  createLocalFirstJwtAsync,
  createDb,
  LOCAL_FIRST_JWT_ISSUER,
  parseJwtPayload,
  resolveClientSessionSync,
  type DbOptions,
} from "./jazz-tools.js";
import { createLocalFirstJwt, localFirstJwtPublicKeyPem } from "./auth.js";

const emptySchema = {};

class FakeWasmDb {
  static openMemory(): FakeWasmDb {
    return new FakeWasmDb();
  }
}

function toBase64Url(value: unknown): string {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function makeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "none", typ: "JWT" })}.${toBase64Url(payload)}.`;
}

function testOptions(options: Partial<DbOptions>): DbOptions {
  return {
    schema: emptySchema,
    Runtime: FakeWasmDb,
    ...options,
  };
}

test("local-first secret deterministically derives the same session", async () => {
  const first = await createDb(testOptions({ appId: "auth-app", secret: "stable-secret" }));
  const second = await createDb(testOptions({ appId: "auth-app", secret: "stable-secret" }));
  const third = await createDb(testOptions({ appId: "auth-app", secret: "other-secret" }));

  assert.equal(first.getAuthState().authMode, "local-first");
  assert.equal(first.getAuthState().session?.authMode, "local-first");
  assert.equal(first.getAuthState().session?.user_id, second.getAuthState().session?.user_id);
  assert.notEqual(first.getAuthState().session?.user_id, third.getAuthState().session?.user_id);
});

test("JWT sub maps to session user_id and claims", () => {
  const jwt = makeJwt({
    sub: "user-subject",
    iss: "https://issuer.example",
    claims: { role: "editor" },
  });

  assert.deepEqual(resolveClientSessionSync({ appId: "auth-app", jwtToken: jwt }), {
    user_id: "user-subject",
    claims: {
      role: "editor",
      subject: "user-subject",
      issuer: "https://issuer.example",
    },
    authMode: "external",
  });
});

test("JWT audience must match the configured app id", () => {
  const jwt = makeJwt({
    sub: "user-subject",
    iss: "https://issuer.example",
    aud: "other-app",
  });

  assert.equal(resolveClientSessionSync({ appId: "auth-app", jwtToken: jwt }), null);
});

test("local-first JWT payload uses the expected issuer", async () => {
  const db = await createDb(testOptions({ appId: "auth-app", secret: "stable-secret" }));
  const state = db.getAuthState();

  assert.equal(state.session?.claims.issuer, LOCAL_FIRST_JWT_ISSUER);
  assert.equal(state.session?.claims.audience, "auth-app");
  assert.equal(
    parseJwtPayload(makeJwt({ sub: state.session?.user_id }))?.sub,
    state.session?.user_id,
  );
});

test("local-first proof is an EdDSA JWT signed by the derived secret key", () => {
  const jwt = createLocalFirstJwt({
    appId: "auth-app",
    secret: "stable-secret",
    subject: "alice",
    now: Date.UTC(2026, 0, 1, 0, 0, 0),
    ttlSeconds: 60,
  });
  const [header, payload, signature] = jwt.split(".");

  assert.deepEqual(JSON.parse(Buffer.from(header, "base64url").toString("utf8")), {
    alg: "EdDSA",
    typ: "JWT",
  });
  assert.deepEqual(parseJwtPayload(jwt), {
    sub: "alice",
    iss: LOCAL_FIRST_JWT_ISSUER,
    exp: 1767225660,
    aud: "auth-app",
    appId: "auth-app",
  });
  assert.equal(
    verify(
      null,
      Buffer.from(`${header}.${payload}`),
      localFirstJwtPublicKeyPem("auth-app", "stable-secret"),
      Buffer.from(signature, "base64url"),
    ),
    true,
  );
});

test("local-first async WebCrypto JWT matches sync payload and verifies", async (t) => {
  const options = {
    appId: "auth-app",
    secret: "stable-secret",
    subject: "alice",
    now: Date.UTC(2026, 0, 1, 0, 0, 0),
    ttlSeconds: 60,
  };
  let jwt: string;

  try {
    jwt = await createLocalFirstJwtAsync(options);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (/WebCrypto|Ed25519|not supported|Unrecognized algorithm/i.test(message)) {
      t.skip(`WebCrypto Ed25519 unavailable: ${message}`);
      return;
    }
    throw error;
  }

  const syncJwt = createLocalFirstJwt(options);
  const [header, payload, signature] = jwt.split(".");
  assert.equal(`${header}.${payload}`, syncJwt.split(".").slice(0, 2).join("."));
  assert.deepEqual(parseJwtPayload(jwt), parseJwtPayload(syncJwt));
  assert.equal(parseJwtPayload(jwt)?.aud, "auth-app");
  assert.equal(
    verify(
      null,
      Buffer.from(`${header}.${payload}`),
      localFirstJwtPublicKeyPem("auth-app", "stable-secret"),
      Buffer.from(signature, "base64url"),
    ),
    true,
  );
});

test("auth state subscription emits immediately and on same-principal token update", async () => {
  const initial = makeJwt({ sub: "alice", claims: { role: "reader" } });
  const refreshed = makeJwt({ sub: "alice", claims: { role: "writer" } });
  const db = await createDb(testOptions({ appId: "auth-app", jwtToken: initial }));
  const states = [];

  const stop = db.onAuthChanged((state) => {
    states.push(state);
  });
  db.updateAuthToken(refreshed);
  stop();

  assert.equal(states.length, 2);
  assert.equal(db.getAuthState().session?.user_id, "alice");
  assert.equal(db.getAuthState().session?.claims.role, "writer");
  assert.equal(db.getAuthState().error, undefined);
});

test("redundant token update does not emit", async () => {
  const jwt = makeJwt({ sub: "alice", claims: { role: "reader" } });
  const db = await createDb(testOptions({ appId: "auth-app", jwtToken: jwt }));
  const states = [];

  const stop = db.onAuthChanged((state) => {
    states.push(state);
  });
  db.updateAuthToken(jwt);
  stop();

  assert.equal(states.length, 1);
});
