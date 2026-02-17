import type {
  AgentSecret,
  RawCoID,
  RawCoMap,
  SessionID,
  SyncMessage,
} from "cojson";
import {
  cojsonInternals,
  ControlledAgent,
  LocalNode,
  StorageApiAsync,
  StorageAPI,
} from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { onTestFinished } from "vitest";

const { knownStateFromContent } = cojsonInternals;

export function trackMessages() {
  const messages: {
    from: "client" | "server" | "storage";
    msg: SyncMessage;
  }[] = [];

  const originalLoad = StorageApiAsync.prototype.load;
  const originalStore = StorageApiAsync.prototype.store;

  StorageApiAsync.prototype.load = async function (id, callback, done) {
    messages.push({
      from: "client",
      msg: {
        action: "load",
        id: id as RawCoID,
        header: false,
        sessions: {},
      },
    });
    return originalLoad.call(
      this,
      id,
      (msg) => {
        messages.push({
          from: "storage",
          msg,
        });
        callback(msg);
      },
      done,
    );
  };

  StorageApiAsync.prototype.store = async function (data, correctionCallback) {
    messages.push({
      from: "client",
      msg: data,
    });

    return originalStore.call(this, data, (msg) => {
      messages.push({
        from: "storage",
        msg: {
          action: "known",
          isCorrection: true,
          ...msg,
        },
      });
      const correctionMessages = correctionCallback(msg);

      if (correctionMessages) {
        for (const msg of correctionMessages) {
          messages.push({
            from: "client",
            msg,
          });
        }
      }

      return correctionMessages;
    });
  };

  const restore = () => {
    StorageApiAsync.prototype.load = originalLoad;
    StorageApiAsync.prototype.store = originalStore;
    messages.length = 0;
  };

  const clear = () => {
    messages.length = 0;
  };

  onTestFinished(() => {
    restore();
  });

  return {
    messages,
    restore,
    clear,
  };
}

export function getAllCoValuesWaitingForDelete(
  storage: StorageAPI,
): Promise<RawCoID[]> {
  // @ts-expect-error - dbClient is private
  return storage.dbClient.getAllCoValuesWaitingForDelete();
}

export async function getCoValueStoredSessions(
  storage: StorageAPI,
  id: RawCoID,
): Promise<SessionID[]> {
  return new Promise<SessionID[]>((resolve) => {
    storage.load(
      id,
      (content) => {
        if (content.id === id) {
          resolve(
            Object.keys(knownStateFromContent(content).sessions) as SessionID[],
          );
        }
      },
      () => {},
    );
  });
}

export function waitFor(
  callback: () => boolean | undefined | Promise<boolean | undefined>,
) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = async () => {
      try {
        return { ok: await callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(async () => {
      const { ok, error } = await checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}

export function fillCoMapWithLargeData(map: RawCoMap) {
  const dataSize = 1 * 1024 * 200;
  const chunkSize = 1024; // 1KB chunks
  const chunks = dataSize / chunkSize;

  const value = btoa(
    new Array(chunkSize).fill("value$").join("").slice(0, chunkSize),
  );

  for (let i = 0; i < chunks; i++) {
    const key = `key${i}`;
    map.set(key, value, "trusting");
  }

  return map;
}

const Crypto = await WasmCrypto.create();

export function getAgentAndSessionID(
  secret: AgentSecret = Crypto.newRandomAgentSecret(),
): [ControlledAgent, SessionID] {
  const sessionID = Crypto.newRandomSessionID(Crypto.getAgentID(secret));
  return [new ControlledAgent(secret, Crypto), sessionID];
}

export function createTestNode(opts?: {
  secret?: AgentSecret;
  enableFullStorageReconciliation?: boolean;
}) {
  const [admin, session] = getAgentAndSessionID(opts?.secret);
  return new LocalNode(
    admin.agentSecret,
    session,
    Crypto,
    undefined,
    opts?.enableFullStorageReconciliation,
  );
}

export function connectToSyncServer(
  client: LocalNode,
  syncServer: LocalNode,
  skipReconciliation: boolean = false,
): void {
  const [clientPeer, serverPeer] = cojsonInternals.connectedPeers(
    client.currentSessionID,
    syncServer.currentSessionID,
    {
      peer1role: "client",
      peer2role: "server",
      persistent: true,
    },
  );

  client.syncManager.addPeer(serverPeer, skipReconciliation);
  syncServer.syncManager.addPeer(clientPeer, skipReconciliation);
}
