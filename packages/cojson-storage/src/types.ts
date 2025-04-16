import type { CojsonInternalTypes, SessionID } from "cojson";

type RawCoID = CojsonInternalTypes.RawCoID;
type Transaction = CojsonInternalTypes.Transaction;
type Signature = CojsonInternalTypes.Signature;

export type CoValueRow = {
  id: CojsonInternalTypes.RawCoID;
  header: CojsonInternalTypes.CoValueHeader;
};

export type StoredCoValueRow = CoValueRow & { rowID: number };

export type SessionRow = {
  coValue: number;
  sessionID: SessionID;
  lastIdx: number;
  lastSignature: CojsonInternalTypes.Signature;
  bytesSinceLastSignature?: number;
};

export type StoredSessionRow = SessionRow & { rowID: number };

export type TransactionRow = {
  ses: number;
  idx: number;
  tx: CojsonInternalTypes.Transaction;
};

export type SignatureAfterRow = {
  ses: number;
  idx: number;
  signature: CojsonInternalTypes.Signature;
};

export interface DBClientInterface {
  getCoValue(
    coValueId: RawCoID,
  ): Promise<StoredCoValueRow | undefined> | StoredCoValueRow | undefined;

  getCoValueSessions(
    coValueRowId: number,
  ): Promise<StoredSessionRow[]> | StoredSessionRow[];

  getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> | StoredSessionRow | undefined;

  getNewTransactionInSession(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<TransactionRow[]> | TransactionRow[];

  getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<SignatureAfterRow[]> | SignatureAfterRow[];

  addCoValue(
    msg: Pick<CojsonInternalTypes.NewContentMessage, "id" | "header">,
  ): Promise<number> | number;

  addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): Promise<number> | number;

  addTransaction(
    sessionRowID: number,
    idx: number,
    newTransaction: Transaction,
  ): Promise<number> | undefined | unknown;

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: Signature;
  }): Promise<number> | undefined | unknown;

  transaction(callback: () => unknown): Promise<unknown> | undefined;
}

export interface SyncDBClientInterface {
  getCoValue(coValueId: RawCoID): StoredCoValueRow | undefined;

  getCoValueSessions(coValueRowId: number): StoredSessionRow[];

  getNewTransactionInSession(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): TransactionRow[];

  getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): SignatureAfterRow[];

  addCoValue(
    msg: Pick<CojsonInternalTypes.NewContentMessage, "id" | "header">,
  ): number;

  addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): number;

  addTransaction(
    sessionRowID: number,
    idx: number,
    newTransaction: Transaction,
  ): number | undefined | unknown;

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: Signature;
  }): number | undefined | unknown;

  transaction(
    operationsCallback: () => unknown[],
  ): Promise<unknown> | undefined;
}
