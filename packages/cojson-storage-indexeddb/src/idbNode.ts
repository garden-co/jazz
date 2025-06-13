import {
  type IncomingSyncStream,
  type OutgoingSyncQueue,
  type Peer,
  cojsonInternals,
} from "cojson";
import { StorageManagerAsync } from "cojson-storage";
import { IDBClient } from "./idbClient.js";

let DATABASE_NAME = "jazz-storage";

export function internal_setDatabaseName(name: string) {
  DATABASE_NAME = name;
}

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

export class IDBNode {
  private readonly dbClient: IDBClient;
  private readonly syncManager: StorageManagerAsync;

  constructor(
    db: IDBDatabase,
    fromLocalNode: IncomingSyncStream,
    toLocalNode: OutgoingSyncQueue,
  ) {
    this.dbClient = new IDBClient(db);
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
          console.error(e);
        }
      }
    };

    processMessages().catch((e) =>
      console.error("Error in processMessages in IndexedDB", e),
    );
  }

  static async asPeer(
    { localNodeName = "local" }: { localNodeName?: string } | undefined = {
      localNodeName: "local",
    },
  ): Promise<Peer> {
    const [localNodeAsPeer, storageAsPeer] = cojsonInternals.connectedPeers(
      localNodeName,
      "indexedDB",
      {
        peer1role: "client",
        peer2role: "storage",
        crashOnClose: true,
      },
    );

    await IDBNode.open(localNodeAsPeer.incoming, localNodeAsPeer.outgoing);

    return { ...storageAsPeer, priority: 100 };
  }

  static async open(
    fromLocalNode: IncomingSyncStream,
    toLocalNode: OutgoingSyncQueue,
  ) {
    const dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open(DATABASE_NAME, 4);
      request.onerror = () => {
        reject(request.error);
      };
      request.onsuccess = () => {
        resolve(request.result);
      };
      request.onupgradeneeded = async (ev) => {
        const db = request.result;
        if (ev.oldVersion === 0) {
          const coValues = db.createObjectStore("coValues", {
            autoIncrement: true,
            keyPath: "rowID",
          });

          coValues.createIndex("coValuesById", "id", {
            unique: true,
          });

          const sessions = db.createObjectStore("sessions", {
            autoIncrement: true,
            keyPath: "rowID",
          });

          sessions.createIndex("sessionsByCoValue", "coValue");
          sessions.createIndex("uniqueSessions", ["coValue", "sessionID"], {
            unique: true,
          });

          db.createObjectStore("transactions", {
            keyPath: ["ses", "idx"],
          });
        }
        if (ev.oldVersion <= 1) {
          db.createObjectStore("signatureAfter", {
            keyPath: ["ses", "idx"],
          });
        }
      };
    });

    return new IDBNode(await dbPromise, fromLocalNode, toLocalNode);
  }
}
