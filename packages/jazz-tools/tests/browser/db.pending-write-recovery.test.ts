import { expect, it } from "vitest";
import { commands } from "vitest/browser";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import { accountRegistryUrl } from "../../src/accounts/context.js";
import { requestAccountRegistry } from "../../src/accounts/registry-client.js";
import { generateAuthSecret } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { recoveryApp, recoveryPermissions } from "./indexeddb-pending-recovery-fixture.js";
import type { RecoveryConfig, RecoveryRows } from "./indexeddb-pending-recovery-fixture.js";
import { getJazzServerInfo, getJazzServerJwtForUser } from "./testing-server.js";

interface RecoveryCommands {
  recoverPendingIndexedDbWrites(config: RecoveryConfig): Promise<RecoveryRows>;
}

// #2633: subscriber admission must yield while pending-write replay fetches
// cold IndexedDB pages. Small/resident transactions can hide the synchronous spin.
it("recovers external-identity writes with an internal registry stub after an offline cold browser restart", async () => {
  const info = await getJazzServerInfo(crypto.randomUUID());
  await deploy({ ...info, schema: recoveryApp.wasmSchema, permissions: recoveryPermissions });
  const jwtToken = await getJazzServerJwtForUser(crypto.randomUUID(), undefined, info.appId);
  const accounts = await createAccountManager({ appId: info.appId, serverUrl: info.serverUrl });
  await accounts.registerJWT(jwtToken);
  // Capture a real assignment; the remote fixture replays only registry login,
  // then uses normal opaque handles and public createDb with real native transport.
  const registryLoginResponse = await requestAccountRegistry(
    accountRegistryUrl(info.serverUrl, info.appId),
    "login",
    jwtToken,
  );
  const recovery = commands as unknown as RecoveryCommands;
  const rows = await recovery.recoverPendingIndexedDbWrites({
    appId: info.appId,
    serverUrl: info.serverUrl,
    jwtToken,
    registryLoginResponse,
  });
  expect(rows).toEqual({
    marker: 1,
    versions: Array.from({ length: 500 }, (_, index) => index),
  });
}, 180_000);

it("recovers pending writes through self-signed admission after a cold browser restart", async () => {
  const info = await getJazzServerInfo(crypto.randomUUID());
  await deploy({ ...info, schema: recoveryApp.wasmSchema, permissions: recoveryPermissions });
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
