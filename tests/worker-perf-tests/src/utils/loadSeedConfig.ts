import { readFileSync, existsSync } from "node:fs";

import { LocalNode, type RawCoID, type RawCoMap, type RawCoList } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import { WebSocket } from "ws";

import {
  SEED_CONFIG_KEYS,
  type SeedConfig,
  type DurationSeedConfig,
  type BatchSeedConfig,
  type ScenarioType,
} from "../schema.ts";
import { getConfigFilePath } from "./seedHelpers.ts";

/**
 * Read the seed config ID from the config file.
 * Returns null if the file doesn't exist.
 */
export function readConfigId(dbPath: string): RawCoID | null {
  const configFilePath = getConfigFilePath(dbPath);

  if (!existsSync(configFilePath)) {
    return null;
  }

  const configId = readFileSync(configFilePath, "utf-8").trim();
  if (!configId) {
    return null;
  }

  return configId as RawCoID;
}

/**
 * Load a CoList from the node and return its items as an array.
 */
async function loadCoList(
  node: LocalNode,
  listId: RawCoID,
): Promise<RawCoID[]> {
  const coValue = await node.loadCoValueCore(listId, undefined, true);

  if (!coValue.isAvailable()) {
    throw new Error(`Failed to load CoList: ${listId}`);
  }

  await coValue.waitForFullStreaming();

  const core = coValue.getCurrentContent();
  if (core.type !== "colist") {
    throw new Error(`Expected CoList but got ${core.type}: ${listId}`);
  }

  const list = core as RawCoList<RawCoID>;
  const items: RawCoID[] = [];

  for (let i = 0; i < list.entries().length; i++) {
    const entry = list.entries()[i];
    if (entry) {
      items.push(entry.value);
    }
  }

  coValue.unmount();
  return items;
}

/**
 * Load the seed configuration CoValue from the sync server.
 * Returns the parsed config with all the IDs needed for testing.
 */
export async function loadSeedConfig(
  peer: string,
  configId: RawCoID,
): Promise<SeedConfig> {
  const crypto = await WasmCrypto.create();
  const agentSecret = crypto.newRandomAgentSecret();
  const agentID = crypto.getAgentID(agentSecret);

  const node = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(agentID),
    crypto,
  );

  const wsPeer = new WebSocketPeerWithReconnection({
    peer,
    reconnectionTimeout: 100,
    pingTimeout: 60_000,
    addPeer: (p) => node.syncManager.addPeer(p),
    removePeer: () => {},
    WebSocketConstructor: WebSocket as unknown as typeof globalThis.WebSocket,
  });

  wsPeer.enable();
  await wsPeer.waitUntilConnected();

  try {
    const coValue = await node.loadCoValueCore(configId, undefined, true);

    if (!coValue.isAvailable()) {
      throw new Error(`Failed to load seed config: ${configId}`);
    }

    const core = coValue.getCurrentContent();
    if (core.type !== "comap") {
      throw new Error(`Seed config is not a CoMap: ${configId}`);
    }

    const map = core as RawCoMap;

    const scenario = map.get(SEED_CONFIG_KEYS.SCENARIO) as ScenarioType;
    const createdAt = map.get(SEED_CONFIG_KEYS.CREATED_AT) as number;
    const groupId = map.get(SEED_CONFIG_KEYS.GROUP_ID) as RawCoID;

    // Load mapIds CoList
    const mapIdsListId = map.get(SEED_CONFIG_KEYS.MAP_IDS) as RawCoID;
    const mapIds = await loadCoList(node, mapIdsListId);

    if (scenario === "duration") {
      // Load fileIds CoList
      const fileIdsListId = map.get(SEED_CONFIG_KEYS.FILE_IDS) as RawCoID;
      const fileIds = await loadCoList(node, fileIdsListId);

      const pdfName = map.get(SEED_CONFIG_KEYS.PDF_NAME) as string;
      const pdfBytes = map.get(SEED_CONFIG_KEYS.PDF_BYTES) as number;

      const config: DurationSeedConfig = {
        scenario: "duration",
        createdAt,
        groupId,
        fileIds,
        mapIds,
        pdfName,
        pdfBytes,
      };

      coValue.unmount();
      return config;
    } else if (scenario === "batch") {
      const minSize = map.get(SEED_CONFIG_KEYS.MIN_SIZE) as number;
      const maxSize = map.get(SEED_CONFIG_KEYS.MAX_SIZE) as number;

      const config: BatchSeedConfig = {
        scenario: "batch",
        createdAt,
        groupId,
        mapIds,
        minSize,
        maxSize,
      };

      coValue.unmount();
      return config;
    }

    throw new Error(`Unknown scenario: ${scenario}`);
  } finally {
    wsPeer.disable();
    node.gracefulShutdown();
  }
}
