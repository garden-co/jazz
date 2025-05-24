import { type Peer } from "cojson";
import { SQLiteDatabaseDriverAsync, SQLiteNodeBaseAsync } from "cojson-storage";

export interface SQLiteConfig {
  adapter: SQLiteDatabaseDriverAsync;
}

export class SQLiteReactNative extends SQLiteNodeBaseAsync {
  static async asPeer(config: SQLiteConfig): Promise<Peer> {
    if (!config.adapter) {
      throw new Error("SQLite adapter is required");
    }

    return SQLiteNodeBaseAsync.create({
      db: config.adapter,
      localNodeName: "localNode",
    });
  }
}
