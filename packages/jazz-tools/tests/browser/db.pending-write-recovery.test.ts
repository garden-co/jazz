import { expect, it } from "vitest";
import { commands } from "vitest/browser";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
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
it("recovers external-identity protocol writes after an offline cold browser restart", async () => {
  const info = await getJazzServerInfo(crypto.randomUUID());
  await deploy({ ...info, schema: recoveryApp.wasmSchema, permissions: recoveryPermissions });
  const jwtToken = await getJazzServerJwtForUser(crypto.randomUUID(), undefined, info.appId);
  const accounts = await createAccountManager({ appId: info.appId, serverUrl: info.serverUrl });
  await accounts.registerJWT(jwtToken);
  // The remote protocol fixture uses the genuinely enrolled JWT identity,
  // never a copied handle/account configuration. Public external login needs
  // the registry online.
  const recovery = commands as unknown as RecoveryCommands;
  const rows = await recovery.recoverPendingIndexedDbWrites({
    appId: info.appId,
    serverUrl: info.serverUrl,
    jwtToken,
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
