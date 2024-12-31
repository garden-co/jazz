import Database, { Database as DatabaseT } from "better-sqlite3";
import {
  IncomingSyncStream,
  OutgoingSyncQueue,
  Peer,
  SyncMessage,
  cojsonInternals,
} from "cojson";
import { SyncManager, TransactionRow } from "cojson-storage";
import { SQLiteClient } from "./sqliteClient.js";
import {
  transformIncomingMessageFromPeer,
  transformOutgoingMessageToPeer,
} from "./transformers.js";

/**
 * This is to transform outgoing message into older protocol message(s) for backward compatibility
 * TODO To be removed after the protocol is updated in the sync server
 */
class LocalNodeWrapper {
  constructor(private queue: OutgoingSyncQueue) {}

  push(msg: SyncMessage): Promise<unknown> {
    const transformedMessages = transformOutgoingMessageToPeer(msg);
    transformedMessages.map((transformedMessage) => {
      // console.log("🔴 <<<=== SQLite is sending", transformedMessage);
    });

    return Promise.all(
      transformedMessages.map((transformedMessage) => {
        return this.queue.push(transformedMessage);
      }),
    );
  }

  close() {
    return this.queue.close();
  }
}

export class SQLiteNode {
  // ugly public static var to be deleted after new protocol is in effect on all peers
  public static USE_PROTOCOL2 = false;

  private readonly syncManager: SyncManager;
  private readonly dbClient: SQLiteClient;

  constructor(
    db: DatabaseT,
    fromLocalNode: IncomingSyncStream,
    toLocalNode: OutgoingSyncQueue,
  ) {
    this.dbClient = new SQLiteClient(db);
    this.syncManager = new SyncManager(
      this.dbClient,
      new LocalNodeWrapper(toLocalNode),
    );

    const processMessages = async () => {
      for await (const msg of fromLocalNode) {
        try {
          if (msg === "Disconnected" || msg === "PingTimeout") {
            throw new Error("Unexpected Disconnected message");
          }
          // console.log("🟡 <<<=== SQLite is getting", msg);

          await this.syncManager.handleSyncMessage(
            transformIncomingMessageFromPeer(msg),
          );
        } catch (e) {
          console.error(
            new Error(
              `Error reading from localNode, handling msg\n\n${JSON.stringify(
                msg,
                (k, v) =>
                  k === "changes" || k === "encryptedChanges"
                    ? v.slice(0, 20) + "..."
                    : v,
              )}`,
              { cause: e },
            ),
          );
        }
      }
    };

    processMessages().catch((e) =>
      console.error("Error in processMessages in sqlite", e),
    );
  }

  static async asPeer({
    filename,
    trace,
    localNodeName = "local",
  }: {
    filename: string;
    trace?: boolean;
    localNodeName?: string;
  }): Promise<Peer> {
    const [localNodeAsPeer, storageAsPeer] = cojsonInternals.connectedPeers(
      localNodeName,
      "storage",
      { peer1role: "client", peer2role: "storage", trace, crashOnClose: true },
    );

    await SQLiteNode.open(
      filename,
      localNodeAsPeer.incoming,
      localNodeAsPeer.outgoing,
    );

    return { ...storageAsPeer, priority: 100 };
  }

  static async open(
    filename: string,
    fromLocalNode: IncomingSyncStream,
    toLocalNode: OutgoingSyncQueue,
  ) {
    const db = Database(filename);
    db.pragma("journal_mode = WAL");

    const oldVersion = (
      db.pragma("user_version") as [{ user_version: number }]
    )[0].user_version as number;

    console.log("DB version", oldVersion);

    if (oldVersion === 0) {
      console.log("Migration 0 -> 1: Basic schema");
      db.prepare(
        `CREATE TABLE IF NOT EXISTS transactions (
                    ses INTEGER,
                    idx INTEGER,
                    tx TEXT NOT NULL,
                    PRIMARY KEY (ses, idx)
                ) WITHOUT ROWID;`,
      ).run();

      db.prepare(
        `CREATE TABLE IF NOT EXISTS sessions (
                    rowID INTEGER PRIMARY KEY,
                    coValue INTEGER NOT NULL,
                    sessionID TEXT NOT NULL,
                    lastIdx INTEGER,
                    lastSignature TEXT,
                    UNIQUE (sessionID, coValue)
                );`,
      ).run();

      db.prepare(
        `CREATE INDEX IF NOT EXISTS sessionsByCoValue ON sessions (coValue);`,
      ).run();

      db.prepare(
        `CREATE TABLE IF NOT EXISTS coValues (
                    rowID INTEGER PRIMARY KEY,
                    id TEXT NOT NULL UNIQUE,
                    header TEXT NOT NULL UNIQUE
                );`,
      ).run();

      db.prepare(
        `CREATE INDEX IF NOT EXISTS coValuesByID ON coValues (id);`,
      ).run();

      db.pragma("user_version = 1");
      console.log("Migration 0 -> 1: Basic schema - done");
    }

    if (oldVersion <= 1) {
      // fix embarrassing off-by-one error for transaction indices
      console.log(
        "Migration 1 -> 2: Fix off-by-one error for transaction indices",
      );

      const txs = db
        .prepare(`SELECT * FROM transactions`)
        .all() as TransactionRow[];

      for (const tx of txs) {
        db.prepare(`DELETE FROM transactions WHERE ses = ? AND idx = ?`).run(
          tx.ses,
          tx.idx,
        );
        tx.idx -= 1;
        db.prepare(
          `INSERT INTO transactions (ses, idx, tx) VALUES (?, ?, ?)`,
        ).run(tx.ses, tx.idx, tx.tx);
      }

      db.pragma("user_version = 2");
      console.log(
        "Migration 1 -> 2: Fix off-by-one error for transaction indices - done",
      );
    }

    if (oldVersion <= 2) {
      console.log("Migration 2 -> 3: Add signatureAfter");

      db.prepare(
        `CREATE TABLE IF NOT EXISTS signatureAfter (
                    ses INTEGER,
                    idx INTEGER,
                    signature TEXT NOT NULL,
                    PRIMARY KEY (ses, idx)
                ) WITHOUT ROWID;`,
      ).run();

      db.prepare(
        `ALTER TABLE sessions ADD COLUMN bytesSinceLastSignature INTEGER;`,
      ).run();

      db.pragma("user_version = 3");
      console.log("Migration 2 -> 3: Add signatureAfter - done!!");
    }

    return new SQLiteNode(db, fromLocalNode, toLocalNode);
  }
}
