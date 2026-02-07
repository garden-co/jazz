import type {
  CojsonInternalTypes,
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
  RawCoID,
  SessionID,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
  SessionRow,
} from "cojson";
import type { WorkerRequest, WorkerResponse } from "./protocol.js";

/**
 * Main-thread RPC proxy implementing `DBClientInterfaceAsync`.
 *
 * Every method serialises its arguments into a `WorkerRequest`, sends
 * it to the Web Worker via `postMessage`, and returns a `Promise`
 * that resolves when the worker responds.
 */
export class BfTreeClient implements DBClientInterfaceAsync {
  private worker: Worker;
  private nextReqId = 0;
  private pending = new Map<
    number,
    {
      resolve: (value: unknown) => void;
      reject: (error: Error) => void;
    }
  >();

  constructor(worker: Worker) {
    this.worker = worker;
    this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      const { reqId, result, error } = event.data;
      const handler = this.pending.get(reqId);
      if (!handler) return;
      this.pending.delete(reqId);
      if (error) {
        handler.reject(new Error(error));
      } else {
        handler.resolve(result);
      }
    };
  }

  private call<T>(method: string, args: unknown[]): Promise<T> {
    const reqId = this.nextReqId++;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(reqId, {
        resolve: resolve as (v: unknown) => void,
        reject,
      });
      this.worker.postMessage({
        reqId,
        method,
        args,
      } satisfies WorkerRequest);
    });
  }

  getCoValue(coValueId: string) {
    return this.call<StoredCoValueRow | undefined>("getCoValue", [coValueId]);
  }

  upsertCoValue(id: string, header?: CojsonInternalTypes.CoValueHeader) {
    return this.call<number | undefined>("upsertCoValue", [id, header]);
  }

  getCoValueSessions(coValueRowId: number) {
    return this.call<StoredSessionRow[]>("getCoValueSessions", [coValueRowId]);
  }

  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ) {
    return this.call<TransactionRow[]>("getNewTransactionInSession", [
      sessionRowId,
      fromIdx,
      toIdx,
    ]);
  }

  getSignatures(sessionRowId: number, firstNewTxIdx: number) {
    return this.call<SignatureAfterRow[]>("getSignatures", [
      sessionRowId,
      firstNewTxIdx,
    ]);
  }

  getAllCoValuesWaitingForDelete() {
    return this.call<RawCoID[]>("getAllCoValuesWaitingForDelete", []);
  }

  /**
   * Transactions are executed entirely inside the worker.
   *
   * Since bf-tree has no multi-key ACID transactions, each
   * sub-operation is sent as an individual worker call with a
   * `tx.` prefix. The `transaction()` wrapper groups them
   * logically on the main-thread side.
   */
  async transaction(
    callback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>,
  ): Promise<unknown> {
    const txProxy: DBTransactionInterfaceAsync = {
      getSingleCoValueSession: (coValueRowId: number, sessionID: SessionID) =>
        this.call<StoredSessionRow | undefined>("tx.getSingleCoValueSession", [
          coValueRowId,
          sessionID,
        ]),

      markCoValueAsDeleted: (id: RawCoID) =>
        this.call("tx.markCoValueAsDeleted", [id]),

      addSessionUpdate: ({
        sessionUpdate,
        sessionRow,
      }: {
        sessionUpdate: SessionRow;
        sessionRow?: StoredSessionRow;
      }) =>
        this.call<number>("tx.addSessionUpdate", [
          { sessionUpdate, sessionRow },
        ]),

      addTransaction: (
        sessionRowID: number,
        idx: number,
        newTransaction: CojsonInternalTypes.Transaction,
      ) => this.call("tx.addTransaction", [sessionRowID, idx, newTransaction]),

      addSignatureAfter: ({
        sessionRowID,
        idx,
        signature,
      }: {
        sessionRowID: number;
        idx: number;
        signature: CojsonInternalTypes.Signature;
      }) =>
        this.call("tx.addSignatureAfter", [{ sessionRowID, idx, signature }]),

      deleteCoValueContent: (
        coValueRow: Pick<StoredCoValueRow, "rowID" | "id">,
      ) => this.call("tx.deleteCoValueContent", [coValueRow]),
    };

    return callback(txProxy);
  }

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: string; synced: boolean }[],
  ) {
    return this.call<void>("trackCoValuesSyncState", [updates]);
  }

  getUnsyncedCoValueIDs() {
    return this.call<RawCoID[]>("getUnsyncedCoValueIDs", []);
  }

  stopTrackingSyncState(id: RawCoID) {
    return this.call<void>("stopTrackingSyncState", [id]);
  }

  eraseCoValueButKeepTombstone(coValueID: RawCoID) {
    return this.call<unknown>("eraseCoValueButKeepTombstone", [coValueID]);
  }

  getCoValueKnownState(coValueId: string) {
    return this.call<CojsonInternalTypes.CoValueKnownState | undefined>(
      "getCoValueKnownState",
      [coValueId],
    );
  }
}
