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

function createParallelOpsRunner() {
  const ops = new Set<Promise<unknown>>();

  return {
    add: (op: Promise<unknown>) => {
      ops.add(op);
      op.finally(() => {
        ops.delete(op);
      });
    },
    wait() {
      return Promise.race(ops);
    },
    get size() {
      return ops.size;
    },
  };
}

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
      const batch = createParallelOpsRunner();

      for await (const msg of fromLocalNode) {
        try {
          if (msg === "Disconnected" || msg === "PingTimeout") {
            throw new Error("Unexpected Disconnected message");
          }

          if (msg.action === "content") {
            await this.syncManager.handleSyncMessage(msg);
          } else {
            batch.add(this.syncManager.handleSyncMessage(msg));
          }

          if (batch.size > 10) {
            await batch.wait();
          }
        } catch (e) {
          logger.error("Error reading from localNode, handling msg", {
            msg,
            err: e,
          });
        }
      }

      db.closeDb().catch((e) =>
        logger.error("Error closing sqlite", { err: e }),
      );
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
