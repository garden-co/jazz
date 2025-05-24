import type { Peer } from "cojson";
import { SQLiteNodeBase } from "cojson-storage";
import { BetterSqliteDriver } from "./betterSqliteDriver.js";

export class SQLiteNode extends SQLiteNodeBase {
  static async asPeer({
    filename,
    localNodeName = "local",
  }: {
    filename: string;
    localNodeName?: string;
  }): Promise<Peer> {
    const db = new BetterSqliteDriver(filename);

    return SQLiteNodeBase.create({
      db,
      localNodeName,
      maxBlockingTime: 500,
    });
  }
}
