import { CoValueHeader, Transaction } from "./coValueCore.js";
import { Signature } from "./crypto/crypto.js";
import { CoValueCore, LocalNode, RawCoID, SessionID } from "./exports.js";
import { KnownStateMessage } from "./sync.js";

export type StoredSessionLog = {
  transactions: Transaction[];
  signatureAfter: { [txIdx: number]: Signature | undefined };
  lastSignature: Signature;
};

export interface StorageAdapter {
  get(id: RawCoID): Promise<{
    header: CoValueHeader;
    sessions: Map<SessionID, StoredSessionLog>;
  } | null>;

  writeHeader(id: RawCoID, header: CoValueHeader): Promise<void>;

  appendToSession(
    id: RawCoID,
    sessionID: SessionID,
    afterIdx: number,
    tx: Transaction[],
    lastSignature: Signature,
  ): Promise<void>;
}

export class StorageDriver {
  private storageAdapter: StorageAdapter;
  private storedStates: Map<RawCoID, KnownStateMessage> = new Map();
  private node: LocalNode;
  constructor(storageAdapter: StorageAdapter, node: LocalNode) {
    this.storageAdapter = storageAdapter;
    this.node = node;
  }

  async get(id: RawCoID) {
    const storedCoValue = await this.storageAdapter.get(id);

    if (!storedCoValue) {
      return null;
    }

    const core = new CoValueCore(storedCoValue.header, this.node);

    for (const [sessionID, sessionLog] of storedCoValue.sessions) {
      let start = 0;
      for (const [signatureAt, signature] of Object.entries(
        sessionLog.signatureAfter,
      )) {
        if (!signature) {
          throw new Error(
            `Expected signature at ${signatureAt} for session ${sessionID}`,
          );
        }
        core
          .tryAddTransactions(
            sessionID,
            sessionLog.transactions.slice(start, parseInt(signatureAt)),
            undefined,
            signature,
            { skipStorage: true },
          )
          ._unsafeUnwrap();
      }
    }

    return core;
  }

  async set(core: CoValueCore): Promise<void> {
    const currentState = this.storedStates.get(core.id);
    const knownState = core.knownState();

    currentState;
    knownState;
  }
}
