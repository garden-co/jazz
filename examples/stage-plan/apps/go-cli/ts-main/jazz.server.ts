// StagePlan's main backend in TypeScript, for go-cli's end-to-end tests
// (JAZZ_MAIN_BACKEND=ts): createJazz on a memory store, serving /jazz.
//
// - End users sign in with "Bearer token-<user>" (as <user>), like
//   jazz-app's test relay; device accounts sync as local-first guests.
// - Backend clients: a loopback OIDC issuer here mints workload tokens;
//   "go-trusted" is trusted (as: "backend"), "go-scoped" acts as the user
//   "agent".
//
// It runs from a directory with jazz-tools installed and StagePlan's
// jazz.generated.ts and jazz.permissions.json beside it (the test copies
// it into ../react-localfirst). It prints the /jazz URL, then a line of
// JSON with the workload tokens, and runs until SIGINT/SIGTERM.
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { createSign, generateKeyPairSync, randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";
import { acceptDeviceAccounts, createJazz, memory, trustWorkload } from "jazz-tools/backend";
import { app } from "./jazz.generated.js";
import permissions from "./jazz.permissions.json" with { type: "json" };

const audience = "jazz:stage-plan-go";

// The loopback OIDC issuer.
const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const kid = randomUUID();
const issuerServer = createServer((req, res) => {
  const base = issuerURL();
  if (req.url === "/.well-known/openid-configuration") {
    json(res, { issuer: base, jwks_uri: `${base}/jwks` });
  } else if (req.url === "/jwks") {
    json(res, {
      keys: [{ ...publicKey.export({ format: "jwk" }), kid, alg: "RS256", use: "sig" }],
    });
  } else {
    res.writeHead(404).end();
  }
});
await listen(issuerServer);
function issuerURL() {
  return `http://127.0.0.1:${(issuerServer.address() as AddressInfo).port}`;
}

function mint(sub: string): string {
  const now = Math.floor(Date.now() / 1000);
  const b64 = (v: object) => Buffer.from(JSON.stringify(v)).toString("base64url");
  const input = `${b64({ alg: "RS256", typ: "JWT", kid })}.${b64({ iss: issuerURL(), aud: audience, sub, iat: now, exp: now + 3600 })}`;
  const signature = createSign("RSA-SHA256").update(input).sign(privateKey).toString("base64url");
  return `${input}.${signature}`;
}

const jazz = createJazz({
  app,
  permissions,
  store: memory(),
  authenticate: async (request: Request) => {
    const header = request.headers.get("authorization") ?? "";
    const match = /^Bearer token-(.+)$/.exec(header);
    if (!match) return acceptDeviceAccounts(request);
    return { userId: match[1]!, expiresAt: Math.floor(Date.now() / 1000) + 3600 };
  },
  backendClients: [
    trustWorkload({ issuer: issuerURL(), audience, claims: { sub: "go-trusted" }, as: "backend" }),
    trustWorkload({
      issuer: issuerURL(),
      audience,
      claims: { sub: "go-scoped" },
      as: () => ({ userId: "agent", claims: { role: "agent" } }),
    }),
  ],
});

const server = createServer((req: IncomingMessage, res: ServerResponse) =>
  jazz.nodeHandler(req, res, () => res.writeHead(404).end()),
);
await jazz.attach(server);
await listen(server);
const port = (server.address() as AddressInfo).port;
process.stdout.write(`http://127.0.0.1:${port}/jazz\n`);
process.stdout.write(
  `${JSON.stringify({ trusted: mint("go-trusted"), scoped: mint("go-scoped") })}\n`,
);

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, async () => {
    await jazz.close();
    server.close();
    issuerServer.close();
    process.exit(0);
  });
}

function listen(s: ReturnType<typeof createServer>) {
  return new Promise<void>((resolve) => s.listen(0, "127.0.0.1", resolve));
}

function json(res: ServerResponse, body: unknown) {
  res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(body));
}
