import { CoValueHeader, Transaction } from "./coValueCore.js";
import { Signature, StreamingHash } from "./crypto/crypto.js";
import {
  AgentSecret,
  CryptoProvider,
  JsonValue,
  Peer,
  RawCoID,
  SessionID,
  SyncMessage,
} from "./exports.js";
import { getGroupDependentKey, isAgentID } from "./ids.js";
import { parseJSON } from "./jsonStringify.js";
import { StoredSessionLog } from "./storage.js";
import { PeerID } from "./sync.js";
import { accountOrAgentIDfromSessionID } from "./typeUtils/accountOrAgentIDfromSessionID.js";

export type TransactionState =
  | {
      state: "availableInStorage";
    }
  | {
      state: "loadingFromStorage";
    }
  | {
      state: "available";
      tx: Transaction;
      signature: Signature | null;
    }
  | {
      state: "verified";
      tx: Transaction;
      signature: Signature | null;
      validity:
        | { type: "unknown" }
        | { type: "pending" }
        | { type: "valid" }
        | { type: "invalid"; reason: string };
      decryptionState:
        | {
            type: "notDecrypted";
          }
        | {
            type: "decrypted";
            changes: JsonValue[];
          }
        | {
            type: "error";
            error: Error;
          };
      stored: boolean;
    }
  | {
      state: "verificationFailed";
      tx: Transaction;
      signature: Signature | null;
      reason: string;
    };

export type SessionEntry = {
  id: SessionID;
  transactions: TransactionState[];
  streamingHash: StreamingHash | null;
  lastAvailable: number;
  lastDepsAvailable: number;
  lastVerified: number;
  lastDecrypted: number;
};

type KnownState =
  | {
      header: boolean;
      sessions: { [key: SessionID]: number };
    }
  | "unknown"
  | "unavailable";

interface CoValueEntry {
  id: RawCoID;
  header: CoValueHeader | null;
  sessions: { [key: SessionID]: SessionEntry };
  storageState: KnownState | "pending";
  peerState: {
    [key: PeerID]: {
      confirmed: KnownState | "pending";
      optimistic: KnownState;
    };
  };
  listeners: { [key: ListenerID]: KnownState };
  dependents: RawCoID[];
}

export type ListenerID = number;

type LoadMetadataFromStorageEffect = {
  type: "loadMetadataFromStorage";
  id: RawCoID;
};

type LoadTransactionsFromStorageEffect = {
  type: "loadTransactionsFromStorage";
  id: RawCoID;
  sessionID: SessionID;
  from: number;
  to: number;
};

type SendMessageToPeerEffect = {
  type: "sendMessageToPeer";
  id: RawCoID;
  peerID: PeerID;
  message: SyncMessage;
};

type NotifyListenerEffect = {
  type: "notifyListener";
  listenerID: ListenerID;
};

type WriteToStorageEffect = {
  type: "writeToStorage";
  id: RawCoID;
  header: CoValueHeader | null;
  sessions: { [key: SessionID]: StoredSessionLog };
};

function createEmptyCoValueEntry(id: RawCoID): CoValueEntry {
  return {
    id,
    header: null,
    sessions: {},
    storageState: "unknown",
    peerState: {},
    listeners: {},
    dependents: [],
  };
}

export class LocalNode2 {
  coValues: { [key: RawCoID]: CoValueEntry };
  agentSecret: AgentSecret;
  peers: PeerID[];
  crypto: CryptoProvider;

  constructor(agentSecret: AgentSecret, crypto: CryptoProvider) {
    this.agentSecret = agentSecret;
    this.coValues = {};
    this.peers = [];
    this.crypto = crypto;
  }

  addPeer(peerID: PeerID) {
    this.peers.push(peerID);
    for (const coValue of Object.values(this.coValues)) {
      coValue.peerState[peerID] = {
        confirmed: "unknown",
        optimistic: "unknown",
      };
    }
  }

  removePeer(peerID: PeerID) {
    const index = this.peers.indexOf(peerID);
    if (index === -1) {
      throw new Error("Peer not found");
    }
    this.peers.splice(index, 1);
    for (const coValue of Object.values(this.coValues)) {
      delete coValue.peerState[peerID];
    }
  }

  subscribe(id: RawCoID): {
    listenerID: ListenerID;
  } {
    const existing = this.coValues[id];
    if (!existing) {
      const entry = createEmptyCoValueEntry(id);
      entry.listeners[1] = "unknown";
      this.coValues[id] = entry;
      return { listenerID: 1 };
    } else {
      const nextListenerID = Object.keys(existing.listeners).length + 1;
      existing.listeners[nextListenerID] = "unknown";
      return { listenerID: nextListenerID };
    }
  }

  unsubscribe(id: RawCoID, listenerID: ListenerID) {
    const existing = this.coValues[id];
    if (!existing) {
      throw new Error("CoValue not found");
    }
    delete existing.listeners[listenerID];
  }

  tick(): {
    effects: (
      | NotifyListenerEffect
      | SendMessageToPeerEffect
      | LoadMetadataFromStorageEffect
      | LoadTransactionsFromStorageEffect
      | WriteToStorageEffect
    )[];
  } {
    const effects = [];

    effects.push(...this.stageLoad().effects);
    this.stageVerify();
    this.stageValidate();
    effects.push(...this.stageNotify().effects);
    effects.push(...this.stageSync().effects);
    effects.push(...this.stageStore().effects);
    return { effects };
  }

  stageLoad(): {
    effects: (
      | LoadMetadataFromStorageEffect
      | LoadTransactionsFromStorageEffect
    )[];
  } {
    const effects: (
      | LoadMetadataFromStorageEffect
      | LoadTransactionsFromStorageEffect
    )[] = [];
    for (const coValue of Object.values(this.coValues)) {
      if (coValue.storageState === "unknown") {
        effects.push({ type: "loadMetadataFromStorage", id: coValue.id });
        coValue.storageState = "pending";
      } else if (coValue.storageState === "pending") {
        continue;
      } else if (coValue.storageState === "unavailable") {
        continue;
      } else {
        if (
          Object.keys(coValue.listeners).length == 0 &&
          coValue.dependents.length === 0
        )
          continue;
        for (const [sessionID, session] of Object.entries(coValue.sessions) as [
          SessionID,
          SessionEntry,
        ][]) {
          let firstToLoad = -1;
          let lastToLoad = -1;

          for (let i = 0; i < session.transactions.length; i++) {
            if (session.transactions[i]?.state === "availableInStorage") {
              if (firstToLoad === -1) {
                firstToLoad = i;
              }
              lastToLoad = i;
              session.transactions[i] = { state: "loadingFromStorage" };
            }
          }

          if (firstToLoad !== -1) {
            effects.push({
              type: "loadTransactionsFromStorage",
              id: coValue.id,
              sessionID,
              from: firstToLoad,
              to: lastToLoad,
            });
          }
        }
      }
    }
    return { effects };
  }

  stageLoadDeps() {
    for (const coValue of Object.values(this.coValues)) {
      if (Object.keys(coValue.listeners).length === 0) {
        continue;
      }
      if (coValue.storageState === "pending") {
        continue;
      }
      if (coValue.header?.ruleset.type === "ownedByGroup") {
        const existing = this.coValues[coValue.header.ruleset.group];
        if (existing) {
          if (!existing.dependents.includes(coValue.id)) {
            existing.dependents.push(coValue.id);
          }
        } else {
          const entry = createEmptyCoValueEntry(coValue.header.ruleset.group);
          entry.dependents.push(coValue.id);
          this.coValues[coValue.header.ruleset.group] = entry;
        }
      } else if (coValue.header?.ruleset.type === "group") {
        for (const session of Object.values(coValue.sessions)) {
          for (const tx of session.transactions) {
            if (tx.state === "available" && tx.tx.privacy === "trusting") {
              const changes = parseJSON(tx.tx.changes);
              for (const change of changes) {
                if (
                  typeof change === "object" &&
                  change !== null &&
                  "op" in change &&
                  change.op === "set" &&
                  "key" in change
                ) {
                  const groupDependency = getGroupDependentKey(change.key);
                  if (groupDependency) {
                    const existing = this.coValues[groupDependency];
                    if (existing) {
                      if (!existing.dependents.includes(coValue.id)) {
                        existing.dependents.push(coValue.id);
                      }
                    } else {
                      const entry = createEmptyCoValueEntry(groupDependency);
                      entry.dependents.push(coValue.id);
                      this.coValues[groupDependency] = entry;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  stageVerify() {
    for (const coValue of Object.values(this.coValues)) {
      if (
        coValue.storageState === "pending" ||
        coValue.storageState === "unknown"
      ) {
        continue;
      }
      sessionLoop: for (const [sessionID, session] of Object.entries(
        coValue.sessions,
      ) as [SessionID, SessionEntry][]) {
        if (session.lastVerified == session.lastAvailable) {
          continue;
        }

        const streamingHash =
          session.streamingHash?.clone() ?? new StreamingHash(this.crypto);

        for (
          let i = session.lastVerified + 1;
          i <= session.lastAvailable;
          i++
        ) {
          const txState = session.transactions[i];

          if (txState?.state !== "available") {
            throw new Error(
              `Transaction ${i} is not available in ${coValue.id} ${sessionID}`,
            );
          }

          streamingHash.update(txState.tx);

          if (txState.signature) {
            const hash = streamingHash.digest();
            const authorID = accountOrAgentIDfromSessionID(sessionID);
            if (!isAgentID(authorID)) {
              throw new Error(
                `Author ID ${authorID} is not an agent ID in ${coValue.id} ${sessionID} - not yet handled`,
              );
            }
            const signerID = this.crypto.getAgentSignerID(authorID);
            if (this.crypto.verify(txState.signature, hash, signerID)) {
              for (let v = session.lastVerified + 1; v <= i; v++) {
                session.transactions[v] = {
                  ...(session.transactions[v] as TransactionState & {
                    state: "available";
                  }),
                  state: "verified",
                  validity: { type: "valid" },
                  decryptionState: { type: "decrypted", changes: [] },
                  stored: false,
                };
              }
              session.lastVerified = i;
            } else {
              console.log(
                `Signature verification failed for transaction ${i} in ${coValue.id} ${sessionID}`,
              );

              for (
                let iv = session.lastVerified + 1;
                iv <= session.lastAvailable;
                iv++
              ) {
                session.transactions[iv] = {
                  ...(session.transactions[iv] as TransactionState & {
                    state: "available";
                  }),
                  state: "verificationFailed",
                  reason: "Invalid signature",
                };
              }
              session.lastVerified = session.lastAvailable;
              break sessionLoop;
            }
          }
        }
      }
    }
  }

  stageValidate() {}

  stageDecrypt() {}

  stageNotify(): { effects: NotifyListenerEffect[] } {
    throw new Error("Not implemented");
  }

  stageSync(): { effects: SendMessageToPeerEffect[] } {
    for (const coValue of Object.values(this.coValues)) {
      if (
        coValue.storageState === "pending" ||
        coValue.storageState === "unknown"
      ) {
        continue;
      } else {
        throw new Error("CoValue is not pending or unknown");
      }
    }
    return { effects: [] };
  }

  stageStore(): { effects: WriteToStorageEffect[] } {
    throw new Error("Not implemented");
  }

  onMetadataLoaded(
    id: RawCoID,
    header: CoValueHeader | null,
    knownState: KnownState,
  ) {
    const entry = this.coValues[id];
    if (!entry) {
      throw new Error("CoValue not found");
    }
    if (header) {
      entry.header = header;
    }
    entry.storageState = knownState;
    if (knownState !== "unknown" && knownState !== "unavailable") {
      for (const sessionID of Object.keys(knownState.sessions) as SessionID[]) {
        let session = entry.sessions[sessionID];
        if (!session) {
          session = {
            id: sessionID,
            transactions: [],
            streamingHash: null,
            lastVerified: -1,
            lastAvailable: -1,
            lastDepsAvailable: -1,
            lastDecrypted: -1,
          };
          entry.sessions[sessionID] = session;
        }
        for (let i = 0; i < (knownState.sessions[sessionID] || 0); i++) {
          if (!session.transactions[i]) {
            session.transactions[i] = { state: "availableInStorage" };
          }
        }
      }
    }
  }

  createCoValue(header: CoValueHeader): {
    effects: (WriteToStorageEffect | SendMessageToPeerEffect)[];
  } {
    throw new Error("Not implemented");
  }

  makeTransaction(
    id: RawCoID,
    sessionID: SessionID,
    tx: Transaction,
  ): {
    result: "success" | "failure";
  } {
    throw new Error("Not implemented");
  }

  addTransaction(
    id: RawCoID,
    sessionID: SessionID,
    after: number,
    transactions: Transaction[],
    signature: Signature,
  ): {
    result: { type: "success" } | { type: "gap"; expectedAfter: number };
  } {
    const entry = this.coValues[id];
    if (!entry) {
      throw new Error("CoValue not found");
    }
    const session = entry.sessions[sessionID];
    if (!session) {
      throw new Error("Session not found");
    }
    if (after > session.transactions.length) {
      return {
        result: { type: "gap", expectedAfter: session.transactions.length },
      };
    }
    for (let i = 0; i < transactions.length; i++) {
      const sessionIdx = after + i;
      if (
        session.transactions[sessionIdx] &&
        session.transactions[sessionIdx]!.state !== "availableInStorage"
      ) {
        throw new Error(
          `Unexpected existing state ${session.transactions[sessionIdx]!.state} at index ${sessionIdx}`,
        );
      }
      session.transactions[sessionIdx] = {
        state: "available",
        tx: transactions[i]!,
        signature: i === transactions.length - 1 ? signature : null,
      };
      session.lastAvailable = Math.max(session.lastAvailable, sessionIdx);
    }
    return { result: { type: "success" } };
  }
}
