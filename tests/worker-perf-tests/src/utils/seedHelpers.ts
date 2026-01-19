import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import { CoValueCore, LocalNode, type RawCoID } from "cojson";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";
import { getBetterSqliteStorage } from "cojson-storage-sqlite";

import { SEED_CONFIG_KEYS, type ScenarioType } from "../schema.ts";

/**
 * Get the path to the config file for a database.
 * The config file stores the seed config CoMap ID.
 */
export function getConfigFilePath(dbPath: string): string {
  return `${dbPath}.config`;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function assertNonEmptyString(
  value: string | undefined,
  label: string,
): string {
  if (!value || value.trim() === "") {
    throw new Error(`Missing required ${label}`);
  }
  return value;
}

export type SeedContext = {
  node: LocalNode;
  group: ReturnType<LocalNode["createGroup"]>;
  configMap: ReturnType<ReturnType<LocalNode["createGroup"]>["createMap"]>;
  dbPath: string;
};

export type SeedResult = {
  db: string;
  scenario: ScenarioType;
  configId: RawCoID;
  groupId: RawCoID;
};

/**
 * Initialize the seed context with a node, group, and config map.
 */
export async function initSeedContext(
  dbPath: string,
  scenario: ScenarioType,
): Promise<SeedContext> {
  await mkdir(dirname(dbPath), { recursive: true });

  const crypto = await NapiCrypto.create();
  const agentSecret = crypto.newRandomAgentSecret();
  const agentID = crypto.getAgentID(agentSecret);

  const node = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(agentID),
    crypto,
  );

  const storage = getBetterSqliteStorage(dbPath);
  node.setStorage(storage);

  const group = node.createGroup();
  group.addMember("everyone", "writer");

  const configMap = group.createMap();
  configMap.set(SEED_CONFIG_KEYS.SCENARIO, scenario);
  configMap.set(SEED_CONFIG_KEYS.CREATED_AT, Date.now());
  configMap.set(SEED_CONFIG_KEYS.GROUP_ID, group.id);

  return { node, group, configMap, dbPath };
}

/**
 * Finalize the seed context and return the result.
 * Also writes the config ID to a file for later retrieval.
 */
export async function finalizeSeedContext(
  ctx: SeedContext,
  scenario: ScenarioType,
): Promise<SeedResult> {
  await sleep(200);
  ctx.node.gracefulShutdown();

  // Write the config ID to a file
  const configFilePath = getConfigFilePath(ctx.dbPath);
  await writeFile(configFilePath, ctx.configMap.id, "utf-8");

  return {
    db: ctx.dbPath,
    scenario,
    configId: ctx.configMap.id,
    groupId: ctx.group.id,
  };
}

/**
 * Helper to sync pending values periodically.
 */
export async function syncPendingValues(
  toSync: { core: CoValueCore }[],
): Promise<void> {
  if (toSync.length >= 10) {
    await Promise.all(toSync.map((value) => value.core.waitForSync()));
    for (const value of toSync) {
      value.core.unmount();
    }
    toSync.length = 0;
  }
}
