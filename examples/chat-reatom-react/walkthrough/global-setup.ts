/**
 * Playwright global setup for walkthrough screenshot capture.
 * Starts a local Jazz server and pushes the schema before the web server launches.
 */
import { join } from "node:path";
import {
  type LocalJazzServerHandle,
  startLocalJazzServer,
  pushSchemaCatalogue,
} from "jazz-tools/testing";
import { WALKTHROUGH_PORT, WALKTHROUGH_APP_ID } from "./walkthrough-constants.js";

const ADMIN_SECRET = "walkthrough-admin-secret";

let serverHandle: LocalJazzServerHandle | null = null;

export default async function globalSetup() {
  serverHandle = await startLocalJazzServer({
    appId: WALKTHROUGH_APP_ID,
    port: WALKTHROUGH_PORT,
    adminSecret: ADMIN_SECRET,
    healthTimeoutMs: 10_000,
    enableLogs: true,
  });

  await pushSchemaCatalogue({
    serverUrl: serverHandle.url,
    appId: WALKTHROUGH_APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, ".."),
  });

  return async () => {
    await serverHandle?.stop();
  };
}
