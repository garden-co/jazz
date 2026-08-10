import { startLocalJazzServer } from "jazz-tools/testing";
import { encodeSchema } from "../../../packages/jazz-tools/dist/runtime/native-runtime/native-runtime-adapter.js";
import { app } from "../schema.ts";

const appId = process.env.JAZZ_APP_ID ?? "00000000-0000-0000-0000-000000000002";
const port = Number(process.env.JAZZ_PORT ?? "1625");
const adminSecret = process.env.JAZZ_ADMIN_SECRET ?? "jazz-rn-e2e-admin";

const server = await startLocalJazzServer({
  appId,
  port,
  inMemory: true,
  allowLocalFirstAuth: true,
  adminSecret,
  schema: encodeSchema(app.wasmSchema),
});

console.log(
  JSON.stringify({
    appId: server.appId,
    serverUrl: server.url,
    adminSecret: server.adminSecret,
  }),
);

const keepAlive = setInterval(() => {}, 60_000);
await new Promise((resolve) => {
  process.once("SIGINT", resolve);
  process.once("SIGTERM", resolve);
});
clearInterval(keepAlive);
await server.stop();
