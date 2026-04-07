/**
 * Unified dev entrypoint: starts a local jazz server, then launches Vite.
 * In development mode the client auto-publishes the current structural schema
 * on first connect. Ctrl+C tears everything down.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { join } from "node:path";
import { startLocalJazzServer } from "jazz-tools/testing";

const APP_ID = "019d4349-2486-7021-a33e-566b0820c5af";
const PORT = 4200;
const ADMIN_SECRET = "dev-admin-secret-chat-react";
const ROOT = join(import.meta.dirname ?? __dirname, "..");

let vite: ChildProcess | null = null;
let stopping = false;

async function main() {
  console.log("Starting jazz server...");
  const server = await startLocalJazzServer({
    appId: APP_ID,
    port: PORT,
    adminSecret: ADMIN_SECRET,
    enableLogs: true,
  });
  console.log(`Jazz server ready at ${server.url}`);

  vite = spawn("npx", ["vite"], {
    cwd: ROOT,
    stdio: "inherit",
    env: {
      ...process.env,
      VITE_JAZZ_SERVER_URL: server.url,
      VITE_JAZZ_APP_ID: APP_ID,
    },
  });

  vite.on("exit", (code) => {
    if (!stopping) {
      console.log(`Vite exited (code ${code}), shutting down...`);
      server.stop();
      process.exit(code ?? 0);
    }
  });

  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    console.log("\nShutting down...");
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
