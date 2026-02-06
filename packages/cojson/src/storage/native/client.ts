/**
 * Native storage client implementing DBClientInterfaceSync.
 *
 * This client wraps the native Rust storage backend (NAPI or WASM)
 * and provides a TypeScript interface compatible with StorageApiSync.
 */

import type {
  CoValueHeader,
  Transaction,
} from "../../coValueCore/verifiedState.js";
import type { Signature } from "../../crypto/crypto.js";
import type { RawCoID, SessionID } from "../../exports.js";
import type { CoValueKnownState } from "../../knownState.js";
import type { PeerID } from "../../sync.js";
import { logger } from "../../logger.js";
import type {
  DBClientInterfaceSync,
  DBTransactionInterfaceSync,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "../types.js";
import type {
  NativeStorageNapi,
  NativeStorageWasm,
  JsStoredCoValueRow,
  JsStoredSessionRow,
  JsTransactionRow,
  JsSignatureRow,
  JsCoValueKnownState,
} from "./types.js";

/**
 * Checks if the storage is WASM-based (uses JSON string APIs).
 */
function isWasmStorage(
  storage: NativeStorageNapi | NativeStorageWasm,
): storage is NativeStorageWasm {
  return "supportsOpfs" in storage;
}

/**
 * Native storage client that implements the synchronous DB client interface.
 */
export class NativeClient
  implements DBClientInterfaceSync, DBTransactionInterfaceSync
{
  private readonly storage: NativeStorageNapi | NativeStorageWasm;
  private readonly isWasm: boolean;

  constructor(storage: NativeStorageNapi | NativeStorageWasm) {
    this.storage = storage;
    this.isWasm = isWasmStorage(storage);
  }

  getCoValue(coValueId: RawCoID): StoredCoValueRow | undefined {
    try {
      if (this.isWasm) {
        const json = (this.storage as NativeStorageWasm).getCovalue(coValueId);
        if (!json) return undefined;

        const row = JSON.parse(json) as JsStoredCoValueRow;
        return this.convertCoValueRow(row);
      } else {
        const row = (this.storage as NativeStorageNapi).getCovalue(coValueId);
        if (!row) return undefined;

        return this.convertCoValueRow(row);
      }
    } catch (e) {
      logger.warn("Failed to get CoValue from native storage", {
        id: coValueId,
        err: e,
      });
      return undefined;
    }
  }

  private convertCoValueRow(row: JsStoredCoValueRow): StoredCoValueRow {
    try {
      const header = JSON.parse(row.headerJson) as CoValueHeader;
      return {
        rowID: row.rowId,
        id: row.id as RawCoID,
        header,
      };
    } catch (e) {
      logger.warn("Invalid JSON in native storage header", {
        id: row.id,
        err: e,
      });
      throw e;
    }
  }

  getCoValueSessions(coValueRowId: number): StoredSessionRow[] {
    try {
      if (this.isWasm) {
        const json = (this.storage as NativeStorageWasm).getCovalueSessions(
          coValueRowId,
        );
        const rows = JSON.parse(json) as JsStoredSessionRow[];
        return rows.map((r) => this.convertSessionRow(r));
      } else {
        const rows = (this.storage as NativeStorageNapi).getCovalueSessions(
          coValueRowId,
        );
        return rows.map((r) => this.convertSessionRow(r));
      }
    } catch (e) {
      logger.warn("Failed to get CoValue sessions from native storage", {
        coValueRowId,
        err: e,
      });
      return [];
    }
  }

  private convertSessionRow(row: JsStoredSessionRow): StoredSessionRow {
    return {
      rowID: row.rowId,
      coValue: row.covalue,
      sessionID: row.sessionId as SessionID,
      lastIdx: row.lastIdx,
      lastSignature: row.lastSignature as Signature,
      bytesSinceLastSignature: row.bytesSinceLastSignature,
    };
  }

  getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): StoredSessionRow | undefined {
    try {
      if (this.isWasm) {
        const json = (
          this.storage as NativeStorageWasm
        ).getSingleCovalueSession(coValueRowId, sessionID);
        if (!json) return undefined;

        const row = JSON.parse(json) as JsStoredSessionRow;
        return this.convertSessionRow(row);
      } else {
        const row = (this.storage as NativeStorageNapi).getSingleCovalueSession(
          coValueRowId,
          sessionID,
        );
        if (!row) return undefined;

        return this.convertSessionRow(row);
      }
    } catch (e) {
      logger.warn("Failed to get single session from native storage", {
        coValueRowId,
        sessionID,
        err: e,
      });
      return undefined;
    }
  }

  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): TransactionRow[] {
    try {
      if (this.isWasm) {
        const json = (
          this.storage as NativeStorageWasm
        ).getNewTransactionInSession(sessionRowId, fromIdx, toIdx);
        const rows = JSON.parse(json) as JsTransactionRow[];
        return rows.map((r) => this.convertTransactionRow(r));
      } else {
        const rows = (
          this.storage as NativeStorageNapi
        ).getNewTransactionInSession(sessionRowId, fromIdx, toIdx);
        return rows.map((r) => this.convertTransactionRow(r));
      }
    } catch (e) {
      logger.warn("Failed to get transactions from native storage", {
        sessionRowId,
        fromIdx,
        toIdx,
        err: e,
      });
      return [];
    }
  }

  private convertTransactionRow(row: JsTransactionRow): TransactionRow {
    try {
      const tx = JSON.parse(row.txJson) as Transaction;
      return {
        ses: row.ses,
        idx: row.idx,
        tx,
      };
    } catch (e) {
      logger.warn("Invalid JSON in native storage transaction", { err: e });
      throw e;
    }
  }

  getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): SignatureAfterRow[] {
    try {
      if (this.isWasm) {
        const json = (this.storage as NativeStorageWasm).getSignatures(
          sessionRowId,
          firstNewTxIdx,
        );
        const rows = JSON.parse(json) as JsSignatureRow[];
        return rows.map((r) => this.convertSignatureRow(r));
      } else {
        const rows = (this.storage as NativeStorageNapi).getSignatures(
          sessionRowId,
          firstNewTxIdx,
        );
        return rows.map((r) => this.convertSignatureRow(r));
      }
    } catch (e) {
      logger.warn("Failed to get signatures from native storage", {
        sessionRowId,
        firstNewTxIdx,
        err: e,
      });
      return [];
    }
  }

  private convertSignatureRow(row: JsSignatureRow): SignatureAfterRow {
    return {
      ses: row.ses,
      idx: row.idx,
      signature: row.signature as Signature,
    };
  }

  upsertCoValue(id: RawCoID, header?: CoValueHeader): number | undefined {
    try {
      const headerJson = header ? JSON.stringify(header) : null;

      if (this.isWasm) {
        const resultJson = (this.storage as NativeStorageWasm).upsertCovalue(
          id,
          headerJson ?? undefined,
        );
        if (!resultJson) return undefined;
        return JSON.parse(resultJson) as number;
      } else {
        const result = (this.storage as NativeStorageNapi).upsertCovalue(
          id,
          headerJson,
        );
        return result ?? undefined;
      }
    } catch (e) {
      logger.warn("Failed to upsert CoValue in native storage", {
        id,
        err: e,
      });
      return undefined;
    }
  }

  markCoValueAsDeleted(id: RawCoID): void {
    try {
      if (this.isWasm) {
        (this.storage as NativeStorageWasm).markCovalueAsDeleted(id);
      } else {
        (this.storage as NativeStorageNapi).markCovalueAsDeleted(id);
      }
    } catch (e) {
      logger.warn("Failed to mark CoValue as deleted in native storage", {
        id,
        err: e,
      });
    }
  }

  eraseCoValueButKeepTombstone(coValueId: RawCoID): void {
    try {
      if (this.isWasm) {
        (this.storage as NativeStorageWasm).eraseCovalueButKeepTombstone(
          coValueId,
        );
      } else {
        (this.storage as NativeStorageNapi).eraseCovalueButKeepTombstone(
          coValueId,
        );
      }
    } catch (e) {
      logger.warn("Failed to erase CoValue in native storage", {
        coValueId,
        err: e,
      });
    }
  }

  getAllCoValuesWaitingForDelete(): RawCoID[] {
    try {
      if (this.isWasm) {
        const json = (
          this.storage as NativeStorageWasm
        ).getAllCovaluesWaitingForDelete();
        return JSON.parse(json) as RawCoID[];
      } else {
        return (
          this.storage as NativeStorageNapi
        ).getAllCovaluesWaitingForDelete() as RawCoID[];
      }
    } catch (e) {
      logger.warn(
        "Failed to get CoValues waiting for delete from native storage",
        { err: e },
      );
      return [];
    }
  }

  addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): number {
    try {
      if (this.isWasm) {
        return (this.storage as NativeStorageWasm).addSession(
          sessionUpdate.coValue,
          sessionUpdate.sessionID,
          sessionUpdate.lastIdx,
          sessionUpdate.lastSignature,
          sessionUpdate.bytesSinceLastSignature,
          sessionRow?.rowID,
        );
      } else {
        return (this.storage as NativeStorageNapi).addSession(
          sessionUpdate.coValue,
          sessionUpdate.sessionID,
          sessionUpdate.lastIdx,
          sessionUpdate.lastSignature,
          sessionUpdate.bytesSinceLastSignature ?? null,
          sessionRow?.rowID ?? null,
        );
      }
    } catch (e) {
      logger.error("Failed to add session update in native storage", {
        sessionUpdate,
        err: e,
      });
      throw e;
    }
  }

  addTransaction(
    sessionRowID: number,
    idx: number,
    newTransaction: Transaction,
  ): number | undefined {
    try {
      const txJson = JSON.stringify(newTransaction);

      if (this.isWasm) {
        return (this.storage as NativeStorageWasm).addTransaction(
          sessionRowID,
          idx,
          txJson,
        );
      } else {
        return (this.storage as NativeStorageNapi).addTransaction(
          sessionRowID,
          idx,
          txJson,
        );
      }
    } catch (e) {
      logger.error("Failed to add transaction in native storage", {
        sessionRowID,
        idx,
        err: e,
      });
      return undefined;
    }
  }

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: Signature;
  }): void {
    try {
      if (this.isWasm) {
        (this.storage as NativeStorageWasm).addSignatureAfter(
          sessionRowID,
          idx,
          signature,
        );
      } else {
        (this.storage as NativeStorageNapi).addSignatureAfter(
          sessionRowID,
          idx,
          signature,
        );
      }
    } catch (e) {
      logger.error("Failed to add signature in native storage", {
        sessionRowID,
        idx,
        err: e,
      });
    }
  }

  transaction(
    operationsCallback: (tx: DBTransactionInterfaceSync) => unknown,
  ): unknown {
    // Native storage handles transactions internally via locking
    // The client itself implements DBTransactionInterfaceSync
    return operationsCallback(this);
  }

  getUnsyncedCoValueIDs(): RawCoID[] {
    try {
      if (this.isWasm) {
        const json = (
          this.storage as NativeStorageWasm
        ).getUnsyncedCovalueIds();
        return JSON.parse(json) as RawCoID[];
      } else {
        return (
          this.storage as NativeStorageNapi
        ).getUnsyncedCovalueIds() as RawCoID[];
      }
    } catch (e) {
      logger.warn("Failed to get unsynced CoValue IDs from native storage", {
        err: e,
      });
      return [];
    }
  }

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
  ): void {
    try {
      const nativeUpdates = updates.map((u) => ({
        id: u.id,
        peerId: u.peerId,
        synced: u.synced,
      }));

      if (this.isWasm) {
        (this.storage as NativeStorageWasm).trackCovaluesSyncState(
          JSON.stringify(nativeUpdates),
        );
      } else {
        (this.storage as NativeStorageNapi).trackCovaluesSyncState(
          nativeUpdates,
        );
      }
    } catch (e) {
      logger.warn("Failed to track sync state in native storage", { err: e });
    }
  }

  stopTrackingSyncState(id: RawCoID): void {
    try {
      if (this.isWasm) {
        (this.storage as NativeStorageWasm).stopTrackingSyncState(id);
      } else {
        (this.storage as NativeStorageNapi).stopTrackingSyncState(id);
      }
    } catch (e) {
      logger.warn("Failed to stop tracking sync state in native storage", {
        id,
        err: e,
      });
    }
  }

  getCoValueKnownState(coValueId: string): CoValueKnownState | undefined {
    try {
      let result: JsCoValueKnownState | null | undefined;

      if (this.isWasm) {
        const json = (this.storage as NativeStorageWasm).getCovalueKnownState(
          coValueId,
        );
        if (!json) return undefined;
        result = JSON.parse(json) as JsCoValueKnownState;
      } else {
        result = (this.storage as NativeStorageNapi).getCovalueKnownState(
          coValueId,
        );
      }

      if (!result) return undefined;

      return {
        id: result.id as RawCoID,
        header: result.header,
        sessions: result.sessions as Record<SessionID, number>,
      };
    } catch (e) {
      logger.warn("Failed to get known state from native storage", {
        coValueId,
        err: e,
      });
      return undefined;
    }
  }
}
