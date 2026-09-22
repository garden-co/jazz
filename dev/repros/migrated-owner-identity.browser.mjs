import { createAccountManager, createDb } from "../../packages/jazz-tools/dist/index.js";
import { oldApp, newApp } from "./migrated-owner-identity.schema.mjs";

let db;
let account;
let config;
const marker = "synthetic B-authored retained row";

window.migratedOwnerIdentity = {
  async initialize(nextConfig) {
    config = nextConfig;
    const accounts = await createAccountManager(config);
    account = await accounts.createLocalFirst();
    db = await createDb({ ...config, account });
    await db.all(oldApp.entries, { tier: "global" });
  },
  async admitMigration() {
    // Keep A open while fetching the published lineage, avoiding #2999's
    // separate offline-new-schema bootstrap boundary.
    await db.all(oldApp.entries, { tier: "global" });
    await db.shutdown();
    db = await createDb({ ...config, account });
    await db.insert(newApp.entries, { text: marker }).wait({ tier: "global" });
    const rows = await db.all(newApp.entries, { tier: "global" });
    if (!rows.some((row) => row.text === marker)) throw new Error("B seed missing");
    await db.shutdown();
    db = undefined;
    return rows;
  },
  async reopenOldLocal() {
    db = await createDb({ ...config, account });
    // Exercise a local read before the strict authority read after reopening.
    // This is a smoke check; it does not reproduce the native identity conflict.
    return await db.all(oldApp.entries, { tier: "local" });
  },
  async readOldGlobal() {
    return await db.all(oldApp.entries, { tier: "global" });
  },
  async close() {
    await db?.shutdown();
  },
};
