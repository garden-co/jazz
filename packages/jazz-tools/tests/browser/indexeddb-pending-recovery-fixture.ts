import { schema as s } from "../../src/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { createAccountManagerWithRuntime } from "../../src/accounts/enrollment.js";
import { accountRegistryUrl } from "../../src/accounts/context.js";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import type { Db } from "../../src/runtime/db.js";

export const recoveryApp = s.defineApp({
  headers: s.table({ version: s.int() }),
  items: s.table({ version: s.int() }),
});
export const recoveryPermissions = s.definePermissions(recoveryApp, ({ policy }) => [
  policy.headers.allowRead.always(),
  policy.headers.allowInsert.always(),
  policy.headers.allowUpdate.always(),
  policy.items.allowRead.always(),
  policy.items.allowInsert.always(),
]);

export type RecoveryConfig = {
  appId: string;
  serverUrl: string;
} & (
  | { jwtToken: string; registryLoginResponse: unknown; secret?: never }
  | { secret: string; jwtToken?: never; registryLoginResponse?: never }
);

export interface RecoveryRows {
  marker: number | undefined;
  versions: number[];
}

const markerId = "018f0000-0000-7000-8000-000000000099";
let db: Db;

export async function open(config: RecoveryConfig): Promise<void> {
  const driver = { type: "persistent" as const, dbName: `pending-recovery-${config.appId}` };
  if (config.secret) {
    const accounts = await createAccountManager({
      appId: config.appId,
      serverUrl: config.serverUrl,
    });
    db = await createDb({
      appId: config.appId,
      serverUrl: config.serverUrl,
      driver,
      account: accounts.restoreLocalFirst(config.secret),
    });
    return;
  }
  // Internal registry-stub protocol regression: replay only the genuine login
  // response captured after online enrollment. External offline login is not a
  // public contract. Native transport still contacts the real, stopped server.
  const registry = accountRegistryUrl(config.serverUrl, config.appId);
  const accounts = createAccountManagerWithRuntime({
    registry,
    localFirst: {
      create() {
        throw new Error("External recovery must not create a local-first account");
      },
    },
    fetch: async (input, init) => {
      if (
        input !== `${registry}/login` ||
        init?.method !== "POST" ||
        new Headers(init.headers).get("Authorization") !== `Bearer ${config.jwtToken}`
      ) {
        throw new Error("Unexpected registry request in external recovery fixture");
      }
      return Response.json(config.registryLoginResponse);
    },
  });
  db = await createDb({
    appId: config.appId,
    serverUrl: config.serverUrl,
    driver,
    account: await accounts.loginJWT(config.jwtToken!),
  });
}

export async function seed(): Promise<void> {
  await db.insert(recoveryApp.headers, { version: 0 }, { id: markerId }).wait({ tier: "edge" });
  await db.all(recoveryApp.items, { tier: "local-first" });
}

export async function writePending(): Promise<void> {
  const write = await db.transaction((batch) => {
    for (let version = 0; version < 500; version++) {
      batch.insert(recoveryApp.items, { version });
    }
    batch.update(recoveryApp.headers, markerId, { version: 1 });
  });
  await write.wait({ tier: "local" });
}

export async function read(): Promise<RecoveryRows> {
  // Keep this first query on the public LocalFirst path: admission must finish
  // before either it or the marker read can observe the recovered transaction.
  const rows = await db.all(recoveryApp.items, { tier: "local-first" });
  const marker = await db.one(recoveryApp.headers.where({ id: markerId }), { tier: "local-first" });
  return {
    marker: marker?.version,
    versions: rows.map((row) => row.version).sort((a, b) => a - b),
  };
}
