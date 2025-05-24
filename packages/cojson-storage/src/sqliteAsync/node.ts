import {
  type IncomingSyncStream,
  type OutgoingSyncQueue,
  type Peer,
  cojsonInternals,
  logger,
} from "cojson";
import { StorageManagerAsync } from "../managerAsync.js";
import { getSQLiteMigrationQueries } from "../sqlite/sqliteMigrations.js";
import { SQLiteClientAsync } from "./client.js";
import type { SQLiteDatabaseDriverAsync } from "./types.js";

export class SQLiteNodeBaseAsync {
  private readonly syncManager: StorageManagerAsync;
  private readonly dbClient: SQLiteClientAsync;

  constructor(
    db: SQLiteDatabaseDriverAsync,
    fromLocalNode: IncomingSyncStream,
    toLocalNode: OutgoingSyncQueue,
  ) {
    this.dbClient = new SQLiteClientAsync(db);
    this.syncManager = new StorageManagerAsync(this.dbClient, toLocalNode);

    const processMessages = async () => {
      for await (const msg of fromLocalNode) {
        try {
          if (msg === "Disconnected" || msg === "PingTimeout") {
            throw new Error("Unexpected Disconnected message");
          }

          await this.syncManager.handleSyncMessage(msg);
        } catch (e) {
          logger.error("Error reading from localNode, handling msg", {
            msg,
            err: e,
          });
        }
      }
    };

    processMessages().catch((e) =>
      logger.error("Error in processMessages in sqlite", { err: e }),
    );
  }

  static async create({
    db,
    localNodeName = "local",
  }: {
    db: SQLiteDatabaseDriverAsync;
    localNodeName?: string;
  }): Promise<Peer> {
    const [localNodeAsPeer, storageAsPeer] = cojsonInternals.connectedPeers(
      localNodeName,
      "storage",
      { peer1role: "client", peer2role: "storage", crashOnClose: true },
    );

    await db.initialize();

    const rows = await db.query<{ user_version: string }>(
      "PRAGMA user_version",
      [],
    );
    const userVersion = Number(rows[0]?.user_version) ?? 0;

    const migrations = getSQLiteMigrationQueries(userVersion);

    for (const migration of migrations) {
      await db.run(migration, []);
    }

    new SQLiteNodeBaseAsync(
      db,
      localNodeAsPeer.incoming,
      localNodeAsPeer.outgoing,
    );

    return { ...storageAsPeer, priority: 100 };
  }
}
