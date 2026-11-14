import { expect, it } from "vitest";
import { commands } from "vitest/browser";
import { accountRegistryUrl } from "../../src/accounts/context.js";
import { generateAuthSecret } from "../../src/index.js";
import { deploy } from "./deploy.js";
import { recoveryApp, recoveryPermissions } from "./indexeddb-pending-recovery-fixture.js";
import type { RecoveryConfig, RecoveryRows } from "./indexeddb-pending-recovery-fixture.js";
import {
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppUserUrl,
  startJazzApp,
  type JazzServerInfo,
} from "./testing-server.js";

interface RecoveryCommands {
  recoverPendingIndexedDbWrites(config: RecoveryConfig): Promise<RecoveryRows>;
}

// #2633: subscriber admission must yield while pending-write replay fetches
// cold IndexedDB pages. Small/resident transactions can hide the synchronous spin.
it("recovers external-identity writes with an internal registry stub after an offline cold browser restart", async () => {
  const info = await publishRecoveryApp();
  const serverUrl = await jazzAppUserUrl(info, crypto.randomUUID());
  // Capture a real backend sign-in; the remote fixture replays only that,
  // then uses normal opaque handles and public createDb with real native transport.
  const signIn = await jazzAppFetch(
    `${accountRegistryUrl(serverUrl, info.appId)}/backend-sign-in`,
    { method: "POST", headers: { "Content-Type": "application/json" } },
  );
  expect(signIn.status).toBe(200);
  const recovery = commands as unknown as RecoveryCommands;
  const rows = await recovery.recoverPendingIndexedDbWrites({
    appId: info.appId,
    serverUrl,
    backendUrl: info.serverUrl,
    backendSignInResponse: await signIn.json(),
  });
  expect(rows).toEqual({
    marker: 1,
    versions: Array.from({ length: 500 }, (_, index) => index),
  });
}, 180_000);

it("recovers pending writes through self-signed admission after a cold browser restart", async () => {
  const info = await publishRecoveryApp();
  const recovery = commands as unknown as RecoveryCommands;
  const rows = await recovery.recoverPendingIndexedDbWrites({
    appId: info.appId,
    serverUrl: info.serverUrl,
    secret: generateAuthSecret(),
  });
  expect(rows).toEqual({
    marker: 1,
    versions: Array.from({ length: 500 }, (_, index) => index),
  });
}, 180_000);

async function publishRecoveryApp(): Promise<JazzServerInfo> {
  const core = await getJazzServerInfo(crypto.randomUUID());
  const app = { schema: recoveryApp.wasmSchema, permissions: recoveryPermissions };
  await deploy({ ...core, ...app });
  return startJazzApp(core, app);
}
