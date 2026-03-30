import { join } from "node:path";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";

const APP_ID = process.env.NEXT_PUBLIC_APP_ID!;
const BACKEND_SECRET = process.env.BACKEND_SECRET!;
const ADMIN_SECRET = process.env.ADMIN_SECRET!;

const server = await TestingServer.start({
  appId: APP_ID,
  port: 1625,
  adminSecret: ADMIN_SECRET,
  backendSecret: BACKEND_SECRET,
});

await new Promise((resolve) => setTimeout(resolve, 500));

await pushSchemaCatalogue({
  serverUrl: server.url,
  appId: server.appId,
  adminSecret: ADMIN_SECRET,
  schemaDir: join(import.meta.dirname ?? __dirname, "../"),
});

console.log(`Schema catalogue pushed to ${server.url} for app ${server.appId}`);
console.log("Sync server is running. Press Ctrl-C to stop.");

// TestingServer runs on the Rust side, so Node needs an explicit handle to stay alive.
const keepAlive = setInterval(() => {}, 1 << 30);

const stopServer = async (signal: NodeJS.Signals) => {
  try {
    console.log(`\nReceived ${signal}, stopping sync server...`);
    clearInterval(keepAlive);
    await server.stop();
    process.exit(0);
  } catch (error) {
    console.error("Failed to stop sync server cleanly:", error);
    process.exit(1);
  }
};

for (const signal of ["SIGINT", "SIGTERM", "SIGQUIT"] as const) {
  process.once(signal, () => stopServer(signal));
}
