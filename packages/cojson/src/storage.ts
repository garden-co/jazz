import { CoValueHeader, Transaction } from "./coValueCore.js";
import { Signature } from "./crypto/crypto.js";
import {
  CoValueCore,
  JsonValue,
  LocalNode,
  RawCoID,
  SessionID,
  Stringified,
  cojsonInternals,
  logger,
} from "./exports.js";
import { CoValueKnownState, SessionNewContent } from "./sync.js";

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

function getGroupDependedOnCoValues(
  sessions: Map<SessionID, StoredSessionLog>,
) {
  const keys: RawCoID[] = [];

  /**
   * Collect all the signing keys inside the transactions to list all the
   * dependencies required to correctly access the CoValue.
   */
  for (const sessionEntry of sessions.values()) {
    for (const tx of sessionEntry.transactions) {
      if (tx.privacy !== "trusting") continue;

      const changes = safeParseChanges(tx.changes);
      for (const change of changes) {
        if (
          change &&
          typeof change === "object" &&
          "op" in change &&
          change.op === "set" &&
          "key" in change &&
          change.key
        ) {
          const key = cojsonInternals.getGroupDependentKey(change.key);

          if (key) {
            keys.push(key);
          }
        }
      }
    }
  }

  return keys;
}

function getOwnedByGroupDependedOnCoValues(
  id: RawCoID,
  header: CoValueHeader,
  sessions: Map<SessionID, StoredSessionLog>,
) {
  if (header.ruleset.type !== "ownedByGroup") return [];

  const keys: RawCoID[] = [header.ruleset.group];

  /**
   * Collect all the signing keys inside the transactions to list all the
   * dependencies required to correctly access the CoValue.
   */
  for (const sessionID of sessions.keys()) {
    const accountId = cojsonInternals.accountOrAgentIDfromSessionID(sessionID);

    if (cojsonInternals.isAccountID(accountId) && accountId !== id) {
      keys.push(accountId);
    }
  }

  return keys;
}

function safeParseChanges(changes: Stringified<JsonValue[]>) {
  try {
    return cojsonInternals.parseJSON(changes);
  } catch (e) {
    return [];
  }
}

function getDependedOnCoValues({
  id,
  header,
  sessions,
}: {
  id: RawCoID;
  header: CoValueHeader;
  sessions: Map<SessionID, StoredSessionLog>;
}) {
  return header.ruleset.type === "group"
    ? getGroupDependedOnCoValues(sessions)
    : header.ruleset.type === "ownedByGroup"
      ? getOwnedByGroupDependedOnCoValues(id, header, sessions)
      : [];
}
export class StorageDriver {
  public storageAdapter: StorageAdapter;
  private storedStates: Map<RawCoID, CoValueKnownState> = new Map();
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

    await Promise.all(
      getDependedOnCoValues({
        id,
        header: storedCoValue.header,
        sessions: storedCoValue.sessions,
      }).map(async (id) => {
        const coValue = this.node.coValuesStore.get(id);

        if (
          coValue.state.type === "unavailable" ||
          coValue.state.type === "unknown"
        ) {
          await this.node.loadCoValueCore(id);
        } else {
          await coValue.getCoValue();
        }
      }),
    );

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

        const position = parseInt(signatureAt) + 1;

        const result = core.tryAddTransactions(
          sessionID,
          sessionLog.transactions.slice(start, position),
          undefined,
          signature,
        );

        if (result.isErr()) {
          console.error(result.error);
          throw result.error;
        }

        start = position;
      }

      if (start < sessionLog.transactions.length) {
        const result = core.tryAddTransactions(
          sessionID,
          sessionLog.transactions.slice(start),
          undefined,
          sessionLog.lastSignature,
        );

        if (result.isErr()) {
          console.error(result.error);
          throw result.error;
        }
      }
    }

    this.storedStates.set(id, core.knownState());

    return core;
  }

  updates: CoValueCore[] = [];
  processing = false;

  set(core: CoValueCore) {
    return this.update(core);
  }

  async processUpdates() {
    while (this.updates.length > 0) {
      const core = this.updates.shift();
      if (!core) {
        continue;
      }

      try {
        await this.update(core);
      } catch (e) {
        logger.error(`Error updating ${core.id}`, {
          error: e instanceof Error ? e.message : String(e),
          stack: (e instanceof Error ? e.stack : undefined) ?? null,
        });
      }
    }
  }

  async update(core: CoValueCore): Promise<void> {
    const currentState = this.storedStates.get(core.id);

    if (!currentState) {
      await this.storageAdapter.writeHeader(core.id, core.header);
    }

    const newContentPieces = core.newContentSince(currentState);

    if (!newContentPieces) {
      return;
    }

    const knownState = core.knownState();

    for (const piece of newContentPieces) {
      const entries = Object.entries(piece.new) as [
        keyof typeof piece.new,
        SessionNewContent,
      ][];

      for (const [sessionID, sessionNewContent] of entries) {
        await this.storageAdapter.appendToSession(
          core.id,
          sessionID,
          sessionNewContent.after,
          sessionNewContent.newTransactions,
          sessionNewContent.lastSignature,
        );
      }
    }

    this.storedStates.set(core.id, knownState);
  }
}
