import { LocalNode, RawCoList, RawCoMap, type RawCoID } from "cojson";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";
import { getBetterSqliteStorage } from "cojson-storage-sqlite";
import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import { WebSocket } from "ws";

import type { ParsedArgs } from "../../utils/args.ts";
import { getFlagString } from "../../utils/args.ts";
import { readConfigId } from "../../utils/loadSeedConfig.ts";
import {
  assertNonEmptyString,
  getConfigFilePath,
  sleep,
} from "../../utils/seedHelpers.ts";
import { SEED_CONFIG_KEYS } from "../../schema.ts";

/**
 * Push seeded batch data from local SQLite DB to a remote sync server.
 * This syncs all the CoValues to the remote server for later testing.
 */
export async function push(args: ParsedArgs): Promise<void> {
  const dbPath = assertNonEmptyString(
    getFlagString(args, "db") ?? "./batch.db",
    "--db",
  );
  const peer = assertNonEmptyString(getFlagString(args, "peer"), "--peer");

  // Read the seed config ID from the config file
  const configId = readConfigId(dbPath);
  if (!configId) {
    throw new Error(
      `No seed config found. Expected config file at: ${getConfigFilePath(dbPath)}. Run seed first.`,
    );
  }

  console.log(
    JSON.stringify(
      {
        action: "push",
        db: dbPath,
        peer,
        configId,
      },
      null,
      2,
    ),
  );

  // Create a node with the local DB as storage
  const crypto = await NapiCrypto.create();
  const agentSecret = crypto.newRandomAgentSecret();
  const agentID = crypto.getAgentID(agentSecret);

  const node = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(agentID),
    crypto,
  );

  // Attach local storage
  const storage = getBetterSqliteStorage(dbPath);
  node.setStorage(storage);

  // Connect to remote sync server
  const wsPeer = new WebSocketPeerWithReconnection({
    peer,
    reconnectionTimeout: 1000,
    pingTimeout: 60_000,
    addPeer: (p) => node.syncManager.addPeer(p),
    removePeer: () => {},
    WebSocketConstructor: WebSocket as unknown as typeof globalThis.WebSocket,
  });

  wsPeer.enable();
  console.log("Connecting to remote sync server...");
  await wsPeer.waitUntilConnected();
  console.log("Connected!");

  try {
    // Load the config map to get the list of map IDs
    console.log("Loading seed configuration...");
    const configValue = await node.loadCoValueCore(configId, undefined, true);

    if (!configValue.isAvailable()) {
      throw new Error(`Failed to load seed config: ${configId}`);
    }

    const configContent = configValue.getCurrentContent() as RawCoMap;
    if (configContent.type !== "comap") {
      throw new Error(`Seed config is not a CoMap: ${configId}`);
    }

    const mapIdsListId = configContent.get(SEED_CONFIG_KEYS.MAP_IDS) as RawCoID;
    const groupId = configContent.get(SEED_CONFIG_KEYS.GROUP_ID) as RawCoID;

    // Load the group
    console.log("Loading group...");
    const groupValue = await node.loadCoValueCore(groupId, undefined, true);
    if (!groupValue.isAvailable()) {
      throw new Error(`Failed to load group: ${groupId}`);
    }

    // Load the map IDs list
    console.log("Loading map IDs list...");
    const mapIdsValue = await node.loadCoValueCore(
      mapIdsListId,
      undefined,
      true,
    );
    if (!mapIdsValue.isAvailable()) {
      throw new Error(`Failed to load map IDs list: ${mapIdsListId}`);
    }
    await mapIdsValue.waitForFullStreaming();

    const mapIdsContent = mapIdsValue.getCurrentContent() as RawCoList;
    if (mapIdsContent.type !== "colist") {
      throw new Error(`Map IDs is not a CoList: ${mapIdsListId}`);
    }

    const mapIds: RawCoID[] = [];
    for (const entry of mapIdsContent.entries()) {
      mapIds.push(entry.value as RawCoID);
    }

    console.log(`Found ${mapIds.length} maps to sync...`);

    // Load all maps to trigger sync
    let synced = 0;
    const batchSize = 100;

    for (let i = 0; i < mapIds.length; i += batchSize) {
      const batch = mapIds.slice(i, i + batchSize);
      await Promise.all(
        batch.map(async (mapId) => {
          const value = await node.loadCoValueCore(mapId, undefined, true);
          if (value.isAvailable()) {
            synced++;
          }
        }),
      );

      console.log(
        `Synced ${Math.min(i + batchSize, mapIds.length)}/${mapIds.length} maps...`,
      );
    }

    // Wait a bit for sync to complete
    console.log("Waiting for sync to complete...");
    await node.syncManager.waitForAllCoValuesSync(120_000);

    console.log(
      JSON.stringify(
        {
          status: "success",
          mapsSynced: synced,
          totalMaps: mapIds.length,
          configId,
          peer,
        },
        null,
        2,
      ),
    );
  } finally {
    wsPeer.disable();
    node.gracefulShutdown();
  }

  console.log("\nPush complete!");
}
