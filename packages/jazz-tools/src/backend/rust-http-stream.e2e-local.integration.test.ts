// Local check (skipped unless RUST_CLIENT is set): a Rust jazz-app backend
// client of a createJazz main backend that can't upgrade sockets syncs over
// the HTTP stream, and an idle stream session outlives the socket's silence
// deadline. See dev/e2e/rust-http-stream-client/README.md.
import { execFile } from "node:child_process";
import { mkdtempSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { exportJWK, generateKeyPair, SignJWT, type JWTPayload } from "jose";
import { afterAll, beforeAll, expect, it } from "vitest";
import { testApp } from "../testing/index.js";
import { createJazz, memory, trustWorkload } from "./index.js";

const CLIENT = process.env.RUST_CLIENT;
const AUDIENCE = "https://app.example.test";
/** Past the Rust socket's 45 s silence deadline. */
const IDLE_SECS = Number(process.env.IDLE_SECS ?? 50);

let issuer: string;
let sign: (claims: JWTPayload, ttl?: string) => Promise<string>;
let issuerServer: Server | undefined;
beforeAll(async () => {
  if (!CLIENT) return;
  const { publicKey, privateKey } = await generateKeyPair("ES256");
  const jwk = { ...(await exportJWK(publicKey)), kid: "k1", alg: "ES256", use: "sig" };
  issuerServer = createServer((req, res) => {
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
  await new Promise<void>((resolve) => issuerServer!.listen(0, "127.0.0.1", resolve));
  issuer = `http://127.0.0.1:${(issuerServer.address() as AddressInfo).port}`;
  sign = (claims, ttl = "10m") =>
    new SignJWT(claims)
      .setProtectedHeader({ alg: "ES256", kid: "k1" })
      .setIssuer(issuer)
      .setAudience(AUDIENCE)
      .setIssuedAt()
      .setExpirationTime(ttl)
      .sign(privateKey);
});
afterAll(
  () =>
    new Promise<void>((resolve) =>
      issuerServer ? issuerServer.close(() => resolve()) : resolve(),
    ),
);

/** A createJazz main backend that can't upgrade sockets, counting requests. */
async function startBackend() {
  const app = testApp(readFileSync(process.env.RUST_APP_JSON!, "utf8"));
  const jazz = await createJazz({
    app,
    permissions: JSON.parse(
      readFileSync(
        process.env.RUST_APP_JSON!.replace(/jazz\.app\.json$/, "jazz.permissions.json"),
        "utf8",
      ),
    ),
    store: memory(),
    authenticate: () => null,
    backendClients: [
      trustWorkload({ issuer, audience: AUDIENCE, claims: { sub: "ci" }, as: "backend" }),
    ],
  }).ready();
  // No attach(): this backend can't upgrade sockets, as on a host without
  // WebSockets, and answers them with 426 and `x-jazz-stream: 1`.
  const requests = { stream: 0, sends: 0, deletes: 0, socket: 0, refusedSends: 0 };
  const server: Server = createServer((req, res) => {
    const path = req.url ?? "";
    if (path.endsWith("/stream")) requests.stream += 1;
    else if (/\/stream\/[^/]+$/.test(path) && req.method === "POST") {
      requests.sends += 1;
      res.on("finish", () => {
        if (res.statusCode >= 400) requests.refusedSends += 1;
      });
    } else if (/\/stream\/[^/]+$/.test(path) && req.method === "DELETE") requests.deletes += 1;
    else if (path.endsWith("/ws")) requests.socket += 1;
    return jazz.nodeHandler(req, res);
  });
  const counts = { upgraded: 0 };
  server.on("upgrade", (_req, socket) => {
    // Whatever passes an upgrade on must not get one through.
    counts.upgraded += 1;
    socket.end("HTTP/1.1 426 Upgrade Required\r\nx-jazz-stream: 1\r\ncontent-length: 0\r\n\r\n");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return {
    app,
    jazz,
    requests,
    counts,
    serverUrl: `${origin}/jazz`,
    async close() {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
      await jazz.close();
    },
  };
}

async function runClient(env: Record<string, string>, timeoutMs: number) {
  return promisify(execFile)(CLIENT!, [], {
    env: { ...process.env, ...env },
    timeout: timeoutMs,
  }).catch((error) => {
    console.log("CLIENT FAILED", error.stdout, error.stderr);
    throw error;
  });
}

it.skipIf(!CLIENT)(
  "a Rust backend client syncs over the HTTP stream where sockets don't upgrade",
  { timeout: IDLE_SECS * 1000 + 90_000 },
  async () => {
    const backend = await startBackend();
    const { app, jazz, requests, counts } = backend;
    try {
      const { stdout, stderr } = await runClient(
        {
          SERVER_URL: backend.serverUrl,
          APP_ID: jazz.appId,
          TOKEN: await sign({ sub: "ci" }),
          IDLE_SECS: String(IDLE_SECS),
        },
        IDLE_SECS * 1000 + 80_000,
      );
      console.log(stdout, stderr, JSON.stringify({ requests, counts }));
      expect(stdout).toContain("KIND Some(Trusted)");
      expect(stdout).toContain("CONNECTED");
      expect(stdout).toContain(`IDLE_DONE ${IDLE_SECS}`);
      expect(stdout).toContain("CLOSED ok");
      // One session, never dropped while idle.
      expect(stdout).not.toContain("LINK ");
      expect(requests.stream).toBe(1);
      expect(requests.sends).toBeGreaterThan(0);
      // No DELETE is guaranteed: the process exits as it closes, and its
      // response going ends the session here (as a socket's TCP close does).
      expect(requests.deletes).toBeLessThanOrEqual(1);
      expect(counts.upgraded).toBeGreaterThan(0);
      const rows = await jazz.db.all(app.todos.select("title"));
      expect(rows.map((row) => row.title as string).sort()).toEqual(["after idle", "before idle"]);
    } finally {
      await backend.close();
    }
  },
);

/**
 * Short-lived tokens, past whose expiry the client keeps syncing. 90 s: a
 * backend client's token must have 60 s left when it signs in
 * (`backend-clients.ts`).
 */
const TOKEN_TTL_SECS = 90;
const RENEWAL_IDLE_SECS = 150;

it.skipIf(!CLIENT)(
  "a stream session gives way to one with a fresh credential before its token expires",
  { timeout: RENEWAL_IDLE_SECS * 1000 + 90_000 },
  async () => {
    const backend = await startBackend();
    const { app, jazz, requests } = backend;
    // The workload's token file, rewritten with a fresh token every 2 s.
    const dir = mkdtempSync(join(tmpdir(), "rust-stream-token-"));
    const tokenFile = join(dir, "token");
    // Written beside it and renamed over it, so the client never reads a
    // half-written token.
    const refresh = async () => {
      const next = `${tokenFile}.next`;
      writeFileSync(next, await sign({ sub: "ci" }, `${TOKEN_TTL_SECS}s`));
      renameSync(next, tokenFile);
    };
    await refresh();
    const timer = setInterval(() => void refresh(), 2_000);
    try {
      const { stdout, stderr } = await runClient(
        {
          SERVER_URL: backend.serverUrl,
          APP_ID: jazz.appId,
          TOKEN_FILE: tokenFile,
          IDLE_SECS: String(RENEWAL_IDLE_SECS),
        },
        RENEWAL_IDLE_SECS * 1000 + 80_000,
      );
      console.log(stdout, stderr, JSON.stringify({ requests }));
      expect(stdout).toContain("KIND Some(Trusted)");
      expect(stdout).toContain(`IDLE_DONE ${RENEWAL_IDLE_SECS}`);
      expect(stdout).toContain("CLOSED ok");
      // Asked again ahead of expiry, past the first token's life.
      expect(stdout.match(/TOKEN_ASKED (?!Connect)/g)?.length ?? 0).toBeGreaterThanOrEqual(1);
      // A renewal reconnects: a new stream session with the new token.
      expect(requests.stream).toBeGreaterThanOrEqual(2);
      // No send ever went with an expired token.
      expect(requests.refusedSends).toBe(0);
      const rows = await jazz.db.all(app.todos.select("title"));
      expect(rows.map((row) => row.title as string).sort()).toEqual(["after idle", "before idle"]);
    } finally {
      clearInterval(timer);
      await backend.close();
    }
  },
);
