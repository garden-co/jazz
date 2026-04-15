import { startLocalJazzServer, pushSchemaCatalogue } from "jazz-tools/testing";
import { join } from "node:path";

const schemaDir = join(import.meta.dirname, "../schema");
const port = 4200;
const adminSecret = "world-tour-dev-secret";

const server = await startLocalJazzServer({
  port,
  adminSecret,
  allowLocalFirstAuth: true,
});

await pushSchemaCatalogue({
  serverUrl: server.url,
  appId: server.appId,
  adminSecret,
  schemaDir,
});

console.log(`Jazz dev server running at ${server.url} (appId: ${server.appId})`);
console.log("Press Ctrl+C to stop");

process.on("SIGINT", async () => {
  await server.stop();
  process.exit(0);
});
