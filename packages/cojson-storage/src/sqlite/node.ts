import {
  type IncomingSyncStream,
  type OutgoingSyncQueue,
  type Peer,
  cojsonInternals,
  logger,
} from "cojson";
import { StorageManagerSync } from "../managerSync.js";
import { SQLiteClient } from "./client.js";
import { getSQLiteMigrationQueries } from "./sqliteMigrations.js";
import type { SQLiteDatabaseDriver } from "./types.js";

export class SQLiteNodeBase {
  private readonly syncManager: StorageManagerSync;
  private readonly dbClient: SQLiteClient;

  constructor(
    db: SQLiteDatabaseDriver,
    fromLocalNode: IncomingSyncStream,
    toLocalNode: OutgoingSyncQueue,
    maxBlockingTime: number,
  ) {
    this.dbClient = new SQLiteClient(db);
    this.syncManager = new StorageManagerSync(this.dbClient, toLocalNode);

    const processMessages = async () => {
      let lastTimer = performance.now();
      let runningTimer = false;

      for await (const msg of fromLocalNode) {
        try {
          if (msg === "Disconnected" || msg === "PingTimeout") {
            throw new Error("Unexpected Disconnected message");
          }

          if (!runningTimer) {
            runningTimer = true;
            lastTimer = performance.now();
            setTimeout(() => {
              runningTimer = false;
            }, 10);
          }

          this.syncManager.handleSyncMessage(msg);

          // Since the DB APIs are synchronous there may be the case
          // where a bulk of messages are processed without interruptions
          // which may block other peers from sending messages.

          // To avoid this we schedule a timer to downgrade the priority of the storage peer work
          if (performance.now() - lastTimer > maxBlockingTime) {
            lastTimer = performance.now();
            await new Promise((resolve) => setTimeout(resolve, 0));
          }
        } catch (e) {
          logger.error("Error reading from localNode, handling msg", {
            msg,
            err: e,
          });
        }
      }

      db.closeDb();
    };

    processMessages().catch((e) =>
      logger.error("Error in processMessages in sqlite", { err: e }),
    );
  }

  static create({
    db,
    localNodeName = "local",
    maxBlockingTime = 500,
  }: {
    db: SQLiteDatabaseDriver;
    localNodeName?: string;
    maxBlockingTime?: number;
  }): Peer {
    const [localNodeAsPeer, storageAsPeer] = cojsonInternals.connectedPeers(
      localNodeName,
      "storage",
      { peer1role: "client", peer2role: "storage", crashOnClose: true },
    );

    const rows = db.query<{ user_version: string }>("PRAGMA user_version", []);
    const userVersion = Number(rows[0]?.user_version) ?? 0;

    const migrations = getSQLiteMigrationQueries(userVersion);

    for (const migration of migrations) {
      db.run(migration, []);
    }

    new SQLiteNodeBase(
      db,
      localNodeAsPeer.incoming,
      localNodeAsPeer.outgoing,
      maxBlockingTime,
    );

    return { ...storageAsPeer, priority: 100 };
  }
}
