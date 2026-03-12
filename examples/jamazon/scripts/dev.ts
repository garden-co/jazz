import { type ChildProcess } from "node:child_process";
import { join } from "node:path";
import { startLocalJazzServer, pushSchemaCatalogue } from "jazz-tools/testing";
import { ADMIN_SECRET, APP_ID, JAZZ_SERVER_PORT } from "@/config";

const ROOT = join(import.meta.dirname ?? __dirname, "..");

let vite: ChildProcess | null = null;
let stopping = false;

async function main() {
  console.log("Starting jazz server...");
  const server = await startLocalJazzServer({
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    port: JAZZ_SERVER_PORT,
    enableLogs: true,
  });
  console.log(`Jazz server ready at ${server.url}`);

  console.log("Pushing schema catalogue...");
  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(ROOT, "schema"),
  });
  console.log("Schema pushed.");

  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    vite?.kill("SIGTERM");
    await server.stop();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
