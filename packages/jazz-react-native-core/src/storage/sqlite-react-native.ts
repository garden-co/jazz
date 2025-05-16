import {
  type IncomingSyncStream,
  type OutgoingSyncQueue,
  type Peer,
  cojsonInternals,
} from "cojson";
import { StorageManagerSync } from "cojson-storage";
import { SQLiteClient } from "./client.js";
import type { SQLiteAdapter } from "./sqlite-adapter.js";

export interface SQLiteConfig {
  adapter: SQLiteAdapter;
}

export class SQLiteReactNative {
  private syncManager!: StorageManagerSync;
  private dbClient!: SQLiteClient;
  private initialized: void;
  private isInitialized = false;

  constructor(
    adapter: SQLiteAdapter,
    fromLocalNode: IncomingSyncStream,
    toLocalNode: OutgoingSyncQueue,
  ) {
    this.initialized = this.initialize(adapter, fromLocalNode, toLocalNode);
  }

  private initialize(
    adapter: SQLiteAdapter,
    fromLocalNode: IncomingSyncStream,
    toLocalNode: OutgoingSyncQueue,
  ): void {
    try {
      // 1. First initialize the adapter
      adapter.initialize();

      // 2. Create and initialize the client
      this.dbClient = new SQLiteClient(adapter, toLocalNode);
      this.dbClient.ensureInitialized();

      // 3. Create the sync manager
      this.syncManager = new StorageManagerSync(this.dbClient, toLocalNode);

      // 4. Start message processing
      this.isInitialized = true;
      this.startMessageProcessing(fromLocalNode);
    } catch (error) {
      console.error("[SQLiteReactNative] initialization failed:", error);
      throw error;
    }
  }

  private async startMessageProcessing(fromLocalNode: IncomingSyncStream) {
    let lastTimer = performance.now();

    try {
      for await (const msg of fromLocalNode) {
        if (!this.isInitialized) {
          this.initialized;
        }

        try {
          if (msg === "Disconnected" || msg === "PingTimeout") {
            throw new Error("Unexpected Disconnected message");
          }

          await this.syncManager.handleSyncMessage(msg);

          if (performance.now() - lastTimer > 500) {
            lastTimer = performance.now();
            await new Promise((resolve) => setTimeout(resolve, 0));
          }
        } catch (e) {
          console.error(
            new Error(
              `Error processing message: ${JSON.stringify(msg, (k, v) =>
                k === "changes" || k === "encryptedChanges"
                  ? `${v.slice(0, 20)}...`
                  : v,
              )}`,
              { cause: e },
            ),
          );
        }
      }
    } catch (e) {
      console.error("Error in message processing loop:", e);
    }
  }

  static async asPeer(config: SQLiteConfig): Promise<Peer> {
    if (!config.adapter) {
      throw new Error("SQLite adapter is required");
    }

    // Initialize adapter before creating any connections
    config.adapter.initialize();

    const [localNodeAsPeer, storageAsPeer] = cojsonInternals.connectedPeers(
      "localNode",
      "storage",
      {
        peer1role: "client",
        peer2role: "storage",
        crashOnClose: true,
      },
    );

    const storage = new SQLiteReactNative(
      config.adapter,
      localNodeAsPeer.incoming,
      localNodeAsPeer.outgoing,
    );

    // Wait for full initialization before returning peer
    storage.initialized;

    return { ...storageAsPeer, priority: 100 };
  }
}
