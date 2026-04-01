import { spawn, type ChildProcess } from "node:child_process";
import { join } from "node:path";
import { TestingServer } from "jazz-tools/testing";

const APP_ID = "00000000-0000-0000-0000-000000000099";
const PORT = 4200;
const ROOT = join(import.meta.dirname ?? __dirname, "..");

let vite: ChildProcess | null = null;
let stopping = false;

async function main() {
  console.log("Starting jazz server...");
  const server = await TestingServer.start({
    appId: APP_ID,
    port: PORT,
  });
  console.log(`Jazz server ready at ${server.url}`);
  console.log("Client will auto-publish the current schema on first connect.");

  vite = spawn("npx", ["vite", "--host"], {
    cwd: ROOT,
    stdio: "inherit",
    env: {
      ...process.env,
      VITE_JAZZ_SERVER_PORT: String(PORT),
      VITE_JAZZ_APP_ID: APP_ID,
    },
  });

  vite.on("exit", (code) => {
    if (!stopping) {
      server.stop();
      process.exit(code ?? 0);
    }
  });

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
