import { createServer, type Server } from "node:http";
import { join } from "node:path";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";
import { TEST_ADMIN_SECRET, TEST_APP_ID } from "./test-constants.js";

let jwksServer: Server | null = null;
let jazzServer: Promise<TestingServer> | null = null;

export async function setup(): Promise<void> {
  if (jazzServer) return;

  const publicJwk = JSON.parse(process.env.JAZZ_TEST_JWKS_PUBLIC_KEY!);
  const jwksPort = Number(process.env.JAZZ_TEST_JWKS_PORT);
  const jazzPort = Number(process.env.JAZZ_TEST_JAZZ_PORT);
  const jwksJson = JSON.stringify({ keys: [publicJwk] });

  jwksServer = createServer((req, res) => {
    if (req.url === "/.well-known/jwks.json") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(jwksJson);
      return;
    }
    res.writeHead(404).end();
  });
  await new Promise<void>((resolve) => jwksServer!.listen(jwksPort, "127.0.0.1", resolve));

  jazzServer = TestingServer.start({
    appId: TEST_APP_ID,
    port: jazzPort,
    adminSecret: TEST_ADMIN_SECRET,
    jwksUrl: `http://127.0.0.1:${jwksPort}/.well-known/jwks.json`,
  });

  const handle = await jazzServer;
  await pushSchemaCatalogue({
    serverUrl: handle.url,
    appId: handle.appId,
    adminSecret: handle.adminSecret,
    schemaDir: join(import.meta.dirname ?? __dirname, "../.."),
  });
}

export async function teardown(): Promise<void> {
  if (jazzServer) await (await jazzServer).stop();
  if (jwksServer) {
    await new Promise<void>((resolve) => jwksServer!.close(() => resolve()));
  }
}
