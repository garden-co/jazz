// A TypeScript main backend for the Rust examples to be backend clients of:
// a stock `createJazz` serving an example's own `jazz.app.json` and
// `jazz.permissions.json`, which trusts machines through `trustWorkload`.
// A stand-in OIDC issuer on loopback signs the machines' tokens, as a CI
// platform's issuer would (GitHub Actions, Vercel, Google Cloud,
// Kubernetes).
//
// The tests start it in-process. To try an example by hand:
//
//   pnpm --filter rust-backend-clients main-backend ../stage-plan/apps/rust-cli
//
// prints the env to export for `cargo run -p <example> -- …`, and keeps
// serving until Ctrl-C.
import { generateKeyPairSync, randomUUID, sign as signWith, type KeyObject } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createJazz, memory, remote, trustWorkload, type Jazz } from "jazz-tools/backend";
import { startLocalJazzServer } from "jazz-tools/dev";
import type { App } from "jazz-tools";
import { testApp, type LooseAppTypes } from "jazz-tools/testing";

/** The audience the main backend expects on machine tokens. */
export const AUDIENCE = "jazz:rust-examples";
/** The `sub` of the scoped machine, and the user the backend maps it to. */
export const SCOPED_SUB = "rust-example";
export const SCOPED_USER = "machine:rust-example";
/** The `sub` of the trusted machine: it acts as the system, and for users. */
export const TRUSTED_SUB = "rust-example-ops";
/** Two of the app's users, with accounts, for a trusted client's `for_user`. */
export const USER_A = "user-a";
export const USER_B = "user-b";

export interface MainBackend {
  jazz: Jazz;
  /** The app as `createJazz` serves it, for reading through `jazz.db`. */
  app: App<LooseAppTypes>;
  /** USER_A's and USER_B's accounts. */
  accounts: { a: string; b: string };
  /** The main backend's Jazz endpoint, for JAZZ_SERVER_URL. */
  serverUrl: string;
  appId: string;
  issuer: string;
  /** A token from the stand-in issuer, for AUDIENCE unless told otherwise. */
  mint(
    claims: Record<string, unknown>,
    options?: { audience?: string; ttlSeconds?: number },
  ): string;
  /**
   * Gives `userId` an account at the core and revokes their identity there:
   * the main backend then answers 403 `identity_not_authorized` for them.
   * For a user this main backend has not met. Only
   * with `{ core: "server" }`, whose core takes admin requests.
   */
  revokeIdentity(userId: string): Promise<void>;
  close(): Promise<void>;
}

export interface MainBackendOptions {
  /**
   * `"memory"` (the default) runs the core in this process; `"server"`
   * runs a local core server the main backend reaches as `remote`, whose
   * admin API `revokeIdentity` uses.
   */
  core?: "memory" | "server";
}

function base64url(data: Buffer | string): string {
  return Buffer.from(data).toString("base64url");
}

/** An ES256 JWT, signed with `key`. */
function jwt(key: KeyObject, payload: Record<string, unknown>): string {
  const head = base64url(JSON.stringify({ alg: "ES256", kid: "k1", typ: "JWT" }));
  const body = base64url(JSON.stringify(payload));
  const signature = signWith("sha256", Buffer.from(`${head}.${body}`), {
    key,
    dsaEncoding: "ieee-p1363",
  });
  return `${head}.${body}.${base64url(signature)}`;
}

function listen(server: Server): Promise<string> {
  return new Promise((done) =>
    server.listen(0, "127.0.0.1", () =>
      done(`http://127.0.0.1:${(server.address() as AddressInfo).port}`),
    ),
  );
}

function shut(server: Server): Promise<void> {
  return new Promise((done) => {
    server.closeAllConnections();
    server.close(() => done());
  });
}

/**
 * Serves the app in `exampleDir` (its `jazz.app.json` and
 * `jazz.permissions.json`). It trusts a token with `sub: TRUSTED_SUB` as a
 * trusted backend client, and one with `sub: SCOPED_SUB` as the user
 * SCOPED_USER (a scoped one). USER_A and USER_B have accounts, for a
 * trusted client to act for (`for_user` never creates one).
 */
export async function startMainBackend(
  exampleDir: string,
  options: MainBackendOptions = {},
): Promise<MainBackend> {
  const app = testApp<LooseAppTypes>(readFileSync(join(exampleDir, "jazz.app.json"), "utf8"));
  const permissions = JSON.parse(readFileSync(join(exampleDir, "jazz.permissions.json"), "utf8"));

  // The stand-in issuer: OpenID discovery and its key set.
  const { publicKey, privateKey } = generateKeyPairSync("ec", { namedCurve: "P-256" });
  const jwk = { ...publicKey.export({ format: "jwk" }), kid: "k1", alg: "ES256", use: "sig" };
  let issuer = "";
  const issuerServer = createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url === "/.well-known/openid-configuration") {
      res.end(JSON.stringify({ issuer, jwks_uri: `${issuer}/jwks` }));
    } else if (req.url === "/jwks") {
      res.end(JSON.stringify({ keys: [jwk] }));
    } else {
      res.statusCode = 404;
      res.end("{}");
    }
  });
  issuer = await listen(issuerServer);

  const appId = randomUUID();
  const core =
    options.core === "server" ? await startLocalJazzServer({ appId, inMemory: true }) : null;
  const jazz = await createJazz({
    app,
    permissions,
    store: core ? remote({ url: core.url, appId, secret: core.adminSecret }) : memory(),
    // The app's people sign in elsewhere; this backend serves machines only.
    authenticate: () => null,
    backendClients: [
      // Trusted: the system's authority, and acting for any user.
      trustWorkload({ issuer, audience: AUDIENCE, claims: { sub: TRUSTED_SUB }, as: "backend" }),
      // Scoped: the example acts as its own user, under the app's permissions.
      trustWorkload({
        issuer,
        audience: AUDIENCE,
        claims: { sub: SCOPED_SUB },
        as: () => ({ userId: SCOPED_USER }),
      }),
    ],
  }).ready();

  const accounts = {
    a: await jazz.createAccountFor(USER_A),
    b: await jazz.createAccountFor(USER_B),
  };

  const server = createServer((req, res) => jazz.nodeHandler(req, res));
  await jazz.attach(server);
  const origin = await listen(server);

  return {
    jazz,
    app,
    accounts,
    serverUrl: `${origin}/jazz`,
    appId: jazz.appId,
    issuer,
    mint(claims, options = {}) {
      const now = Math.floor(Date.now() / 1000);
      return jwt(privateKey, {
        iss: issuer,
        aud: options.audience ?? AUDIENCE,
        iat: now,
        exp: now + (options.ttlSeconds ?? 600),
        jti: randomUUID(),
        ...claims,
      });
    },
    async revokeIdentity(userId) {
      if (!core) throw new Error('revokeIdentity needs startMainBackend(dir, { core: "server" })');
      // The main backend names its users `jazz:backend/<userId>`.
      // Registered and revoked at the core, as TS's own backend-client test
      // does: the main backend has not met the user, so it asks the core.
      const identity = { issuer: "jazz:backend", subject: userId };
      const admin = (operation: string, body: unknown) =>
        fetch(`${core.url}/apps/${appId}/admin/accounts/${operation}`, {
          method: "POST",
          headers: { "X-Jazz-Admin-Secret": core.adminSecret, "content-type": "application/json" },
          body: JSON.stringify(body),
        });
      const registered = await admin("login-or-register", { identity });
      if (registered.status !== 200) {
        throw new Error(`registering ${userId}: the core answered ${registered.status}`);
      }
      const revoked = await admin("revoke", { approver: identity, target: identity });
      if (revoked.status !== 204) {
        throw new Error(`revoking ${userId}'s identity: the core answered ${revoked.status}`);
      }
    },
    async close() {
      await shut(server);
      await jazz.close();
      await core?.stop();
      await shut(issuerServer);
    },
  };
}

if (process.argv[1] && resolve(process.argv[1]) === new URL(import.meta.url).pathname) {
  const exampleDir = resolve(process.argv[2] ?? "../stage-plan/apps/rust-cli");
  const backend = await startMainBackend(exampleDir);
  // Tokens live ten minutes: rewrite the file so a long session keeps going.
  const tokenFile = join(mkdtempSync(join(tmpdir(), "rust-example-token-")), "token");
  const refresh = () => writeFileSync(tokenFile, backend.mint({ sub: SCOPED_SUB }));
  refresh();
  const timer = setInterval(refresh, 5 * 60_000);
  console.log(`export JAZZ_SERVER_URL=${backend.serverUrl}`);
  console.log(`export JAZZ_APP_ID=${backend.appId}`);
  console.log(`export JAZZ_TOKEN_FILE=${tokenFile}`);
  console.log(`# serving ${exampleDir} to ${SCOPED_USER}; Ctrl-C to stop`);
  process.once("SIGINT", () => {
    clearInterval(timer);
    void backend.close().then(() => process.exit(0));
  });
}
