import { join } from "node:path";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";

const APP_ID = process.env.NEXT_PUBLIC_APP_ID!;
const ADMIN_SECRET = process.env.ADMIN_SECRET!;
const BACKEND_SECRET = process.env.BACKEND_SECRET!;
const APP_ORIGIN = process.env.NEXT_PUBLIC_APP_ORIGIN!;

const server = await TestingServer.start({
  appId: APP_ID,
  port: 1625,
  adminSecret: ADMIN_SECRET,
  backendSecret: BACKEND_SECRET,
  jwksUrl: `${APP_ORIGIN}/api/auth/jwks`,
});

await pushSchemaCatalogue({
  serverUrl: server.url,
  appId: APP_ID,
  adminSecret: server.adminSecret,
  schemaDir: join(import.meta.dirname ?? __dirname, "../schema-better-auth"),
});

await pushSchemaCatalogue({
  serverUrl: server.url,
  appId: APP_ID,
  adminSecret: server.adminSecret,
  schemaDir: join(import.meta.dirname ?? __dirname, "../"),
});

console.log(`Schema catalogue pushed to ${server.url} for app ${server.appId}`);
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
  process.once(signal, () => {
    void stopServer(signal);
  });
}
