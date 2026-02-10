import type {
  CoValueHeader,
  Transaction,
} from "cojson/dist/coValueCore/verifiedState.js";
import type { Signature } from "cojson/dist/crypto/crypto.js";
import type { RawCoID, SessionID } from "cojson";
import type { CoValueKnownState } from "cojson/dist/knownState.js";
import type { PeerID } from "cojson/dist/sync.js";
import type {
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson";
import type { FjallStorageNapiTyped } from "./types.js";

/**
 * Transaction implementation that delegates to FjallStorageNapi async methods.
 *
 * Because fjall handles atomicity at the Rust level (write batches),
 * individual operations are just async NAPI calls. The `StoreQueue`
 * in `StorageApiAsync` serializes writes at the JS level.
 */
class FjallTransaction implements DBTransactionInterfaceAsync {
  constructor(private readonly napi: FjallStorageNapiTyped) {}

  async getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> {
    const result = await this.napi.getSingleCoValueSession(
      coValueRowId,
      sessionID,
    );
    if (!result) return undefined;
    return {
      rowID: result.rowId,
      coValue: result.coValue,
      sessionID: result.sessionId as SessionID,
      lastIdx: result.lastIdx,
      lastSignature: result.lastSignature as Signature,
      bytesSinceLastSignature: result.bytesSinceLastSignature,
    };
  }

  async markCoValueAsDeleted(id: RawCoID): Promise<void> {
    await this.napi.markCoValueAsDeleted(id);
  }

  async addSessionUpdate({
    sessionUpdate,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): Promise<number> {
    return this.napi.addSessionUpdate(
      sessionUpdate.coValue,
      sessionUpdate.sessionID,
      sessionUpdate.lastIdx,
      sessionUpdate.lastSignature as string,
      sessionUpdate.bytesSinceLastSignature ?? 0,
    );
  }

  async addTransaction(
    sessionRowID: number,
    idx: number,
    newTransaction: Transaction,
  ): Promise<void> {
    await this.napi.addTransaction(
      sessionRowID,
      idx,
      JSON.stringify(newTransaction),
    );
  }

  async addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: Signature;
  }): Promise<void> {
    await this.napi.addSignatureAfter(sessionRowID, idx, signature as string);
  }

  async deleteCoValueContent(
    coValueRow: Pick<StoredCoValueRow, "rowID" | "id">,
  ): Promise<void> {
    await this.napi.eraseCoValueButKeepTombstone(coValueRow.id);
  }
}

/**
 * Fjall storage client implementing `DBClientInterfaceAsync`.
 *
 * All storage I/O is offloaded to the libuv thread pool via NAPI `AsyncTask`.
 * This keeps the Node.js event loop free for WebSocket traffic and other work.
 */
export class FjallClient implements DBClientInterfaceAsync {
  private readonly napi: FjallStorageNapiTyped;

  constructor(napi: FjallStorageNapiTyped) {
    this.napi = napi;
  }

  async getCoValue(coValueId: string): Promise<StoredCoValueRow | undefined> {
    const result = await this.napi.getCoValue(coValueId);
    if (!result) return undefined;

    try {
      const header = JSON.parse(result.headerJson) as CoValueHeader;
      return {
        rowID: result.rowId,
        id: coValueId as RawCoID,
        header,
      };
    } catch {
      return undefined;
    }
  }

  async upsertCoValue(
    id: string,
    header?: CoValueHeader,
  ): Promise<number | undefined> {
    const headerJson = header ? JSON.stringify(header) : undefined;
    const result = await this.napi.upsertCoValue(id, headerJson ?? undefined);
    return result ?? undefined;
  }

  async getAllCoValuesWaitingForDelete(): Promise<RawCoID[]> {
    return (await this.napi.getAllCoValuesWaitingForDelete()) as RawCoID[];
  }

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    const results = await this.napi.getCoValueSessions(coValueRowId);
    return results.map((r) => ({
      rowID: r.rowId,
      coValue: r.coValue,
      sessionID: r.sessionId as SessionID,
      lastIdx: r.lastIdx,
      lastSignature: r.lastSignature as Signature,
      bytesSinceLastSignature: r.bytesSinceLastSignature,
    }));
  }

  async getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): Promise<TransactionRow[]> {
    const results = await this.napi.getNewTransactionInSession(
      sessionRowId,
      fromIdx,
      toIdx,
    );

    try {
      return results.map((r) => ({
        ses: r.ses,
        idx: r.idx,
        tx: JSON.parse(r.tx) as Transaction,
      }));
    } catch {
      return [];
    }
  }

  async getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<SignatureAfterRow[]> {
    const results = await this.napi.getSignatures(sessionRowId, firstNewTxIdx);
    return results.map((r) => ({
      ses: sessionRowId,
      idx: r.idx,
      signature: r.signature as Signature,
    }));
  }

  async transaction(
    callback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>,
  ): Promise<unknown> {
    // Fjall handles atomicity at the Rust level via write batches.
    // The JS-level FjallTransaction delegates each op to the NAPI layer.
    // The StoreQueue in StorageApiAsync serializes writes.
    const tx = new FjallTransaction(this.napi);
    return callback(tx);
  }

  async trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
  ): Promise<void> {
    await this.napi.trackCoValuesSyncState(
      updates.map((u) => ({
        id: u.id,
        peerId: u.peerId,
        synced: u.synced,
      })),
    );
  }

  async getUnsyncedCoValueIDs(): Promise<RawCoID[]> {
    return (await this.napi.getUnsyncedCoValueIds()) as RawCoID[];
  }

  async stopTrackingSyncState(id: RawCoID): Promise<void> {
    await this.napi.stopTrackingSyncState(id);
  }

  async eraseCoValueButKeepTombstone(coValueID: RawCoID): Promise<void> {
    await this.napi.eraseCoValueButKeepTombstone(coValueID);
  }

  async getCoValueKnownState(
    coValueId: string,
  ): Promise<CoValueKnownState | undefined> {
    const result = await this.napi.getCoValueKnownState(coValueId);
    if (!result) return undefined;

    const knownState: CoValueKnownState = {
      id: coValueId as RawCoID,
      header: true,
      sessions: {},
    };

    for (const [sessionId, lastIdx] of Object.entries(result.sessions)) {
      knownState.sessions[sessionId as SessionID] = lastIdx as number;
    }

    return knownState;
  }
}
