import { join } from "node:path";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";
import { DEFAULT_ADMIN_SECRET, DEFAULT_APP_ID, WORKOS_CLIENT_ID } from "../constants.js";

const server = await TestingServer.start({
  appId: DEFAULT_APP_ID,
  port: 1625,
  adminSecret: DEFAULT_ADMIN_SECRET,
  jwksUrl: `https://api.workos.com/sso/jwks/${WORKOS_CLIENT_ID}`,
});

await pushSchemaCatalogue({
  serverUrl: server.url,
  appId: DEFAULT_APP_ID,
  adminSecret: DEFAULT_ADMIN_SECRET,
  schemaDir: join(import.meta.dirname ?? __dirname, ".."),
});

console.log(`Schema catalogue pushed to ${server.url} for app ${DEFAULT_APP_ID}`);
console.log("Sync server is running. Press Ctrl-C to stop.");

const i = setInterval(() => {}, 10_000_000);

const stopServer = async (signal: NodeJS.Signals) => {
  clearInterval(i);
  try {
    console.log(`\nReceived ${signal}, stopping sync server...`);
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
