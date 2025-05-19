import { type Peer, cojsonInternals } from "cojson";
import { SQLiteDatabaseDriver, SQLiteNodeBase } from "cojson-storage";

export interface SQLiteConfig {
  adapter: SQLiteDatabaseDriver;
}

export class SQLiteReactNative extends SQLiteNodeBase {
  static async asPeer(config: SQLiteConfig): Promise<Peer> {
    if (!config.adapter) {
      throw new Error("SQLite adapter is required");
    }

    const [localNodeAsPeer, storageAsPeer] = cojsonInternals.connectedPeers(
      "localNode",
      "storage",
      {
        peer1role: "client",
        peer2role: "storage",
        crashOnClose: true,
      },
    );

    new SQLiteReactNative(
      config.adapter,
      localNodeAsPeer.incoming,
      localNodeAsPeer.outgoing,
    );

    return { ...storageAsPeer, priority: 100 };
  }
}
