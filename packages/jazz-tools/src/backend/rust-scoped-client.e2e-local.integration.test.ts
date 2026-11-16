// Local-only check (not committed): a Rust jazz-app client as a scoped
// backend client of createJazz, with a workload's OIDC token.
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { promisify } from "node:util";
import { exportJWK, generateKeyPair, SignJWT, type JWTPayload } from "jose";
import { afterAll, beforeAll, expect, it } from "vitest";
import { testApp } from "../testing/index.js";
import { createJazz, memory, trustWorkload } from "./index.js";

const CLIENT = process.env.RUST_CLIENT!;
const app = testApp(readFileSync(process.env.RUST_APP_JSON!, "utf8"));
const permissionsFile = {
  format: 1,
  tables: { todos: { read: "owner = session.user", insert: "owner = session.user" } },
};
const AUDIENCE = "https://app.example.test";

let issuer: string;
let sign: (claims: JWTPayload) => Promise<string>;
let issuerServer: Server;
beforeAll(async () => {
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
  await new Promise<void>((resolve) => issuerServer.listen(0, "127.0.0.1", resolve));
  issuer = `http://127.0.0.1:${(issuerServer.address() as AddressInfo).port}`;
  sign = (claims) =>
    new SignJWT(claims)
      .setProtectedHeader({ alg: "ES256", kid: "k1" })
      .setIssuer(issuer)
      .setAudience(AUDIENCE)
      .setIssuedAt()
      .setExpirationTime("10m")
      .sign(privateKey);
});
afterAll(() => new Promise((resolve) => issuerServer.close(resolve)));

it("a Rust client syncs as a scoped backend client", { timeout: 60_000 }, async () => {
  const jazz = await createJazz({
    app,
    permissions: permissionsFile,
    store: memory(),
    authenticate: () => null,
    backendClients: [
      trustWorkload({
        issuer,
        audience: AUDIENCE,
        claims: { sub: "worker" },
        as: (token) => ({ userId: `machine:${token.sub}` }),
      }),
    ],
  }).ready();
  const server: Server = createServer((req, res) => jazz.nodeHandler(req, res));
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    const { stdout, stderr } = await promisify(execFile)(CLIENT, [], {
      env: {
        ...process.env,
        SERVER_URL: `${origin}/jazz`,
        APP_ID: jazz.appId,
        TOKEN: await sign({ sub: "worker" }),
      },
      timeout: 50_000,
    }).catch((error) => {
      console.log("CLIENT FAILED", error.stdout, error.stderr);
      throw error;
    });
    console.log(stdout, stderr);
    expect(stdout).toContain("SIGNED_IN subject=machine:worker");
    expect(stdout).toContain("REMOTE_ROWS 1");
    const account = /account=(\S+)/.exec(stdout)![1];
    const rows = await jazz.db.all(app.todos);
    expect(rows.map((t: { title: string; owner: string }) => [t.title, t.owner])).toEqual([
      ["from rust", account],
    ]);
    // A token naming another workload is refused at sign-in.
    const refused = await promisify(execFile)(CLIENT, [], {
      env: {
        ...process.env,
        SERVER_URL: `${origin}/jazz`,
        APP_ID: jazz.appId,
        TOKEN: await sign({ sub: "intruder" }),
      },
      timeout: 30_000,
    }).then(
      () => "opened",
      (error) => String(error.stderr),
    );
    console.log("INTRUDER:", refused);
    expect(refused).not.toBe("opened");
  } finally {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await jazz.close();
  }
});
