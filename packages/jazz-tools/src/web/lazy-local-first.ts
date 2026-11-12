import type { AccountDbConfig } from "../accounts/context.js";
import type { AccountManagerConfig } from "../accounts/create-account-manager.js";
import type { JazzClient } from "./registered-jazz-client.js";

// The framework entries re-export these. Loading the local-first client on
// first call keeps its WASM engine out of apps that stay on the thin client.

/** Open (or share) a local-first browser client for an account. */
export async function openJazzClient(config: AccountDbConfig): Promise<JazzClient> {
  const local = await import("./create-jazz-client.js");
  return await local.openJazzClient(config);
}

/** Prepare crypto and restore selection; createLocalFirst() is synchronous afterward. */
export async function createAccountManager(config: AccountManagerConfig) {
  const local = await import("../accounts/create-account-manager.js");
  return await local.createAccountManager(config);
}
