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
import { createJazz, memory, trustWorkload, type Jazz } from "jazz-tools/backend";
import { testApp } from "jazz-tools/testing";

/** The audience the main backend expects on machine tokens. */
export const AUDIENCE = "jazz:rust-examples";
/** The `sub` of the scoped machine, and the user the backend maps it to. */
export const SCOPED_SUB = "rust-example";
export const SCOPED_USER = "machine:rust-example";

export interface MainBackend {
  jazz: Jazz;
  /** The app as `createJazz` serves it, for reading through `jazz.db`. */
  app: ReturnType<typeof testApp>;
  /** The main backend's Jazz endpoint, for JAZZ_SERVER_URL. */
  serverUrl: string;
  appId: string;
  issuer: string;
  /** A token from the stand-in issuer, for AUDIENCE unless told otherwise. */
  mint(claims: Record<string, unknown>, options?: { audience?: string; ttlSeconds?: number }): string;
  close(): Promise<void>;
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
 * `jazz.permissions.json`), trusting a token with `sub: SCOPED_SUB` as the
 * user SCOPED_USER: a scoped backend client.
 */
export async function startMainBackend(exampleDir: string): Promise<MainBackend> {
  const app = testApp(readFileSync(join(exampleDir, "jazz.app.json"), "utf8"));
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

  const jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    // The app's people sign in elsewhere; this backend serves machines only.
    authenticate: () => null,
    backendClients: [
      // Scoped: the example acts as its own user, under the app's permissions.
      trustWorkload({
        issuer,
        audience: AUDIENCE,
        claims: { sub: SCOPED_SUB },
        as: () => ({ userId: SCOPED_USER }),
      }),
    ],
  }).ready();

  const server = createServer((req, res) => jazz.nodeHandler(req, res));
  await jazz.attach(server);
  const origin = await listen(server);

  return {
    jazz,
    app,
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
    async close() {
      await shut(server);
      await jazz.close();
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
