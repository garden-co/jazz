import {
  type CojsonInternalTypes,
  type SessionID,
  cojsonInternals,
} from "cojson";
import type { StoredSessionRow, TransactionRow } from "./types.js";

export function collectNewTxs({
  newTxsInSession,
  contentMessage,
  sessionRow,
  firstNewTxIdx,
  signature,
}: {
  newTxsInSession: TransactionRow[];
  contentMessage: CojsonInternalTypes.NewContentMessage;
  sessionRow: StoredSessionRow;
  signature: CojsonInternalTypes.Signature;
  firstNewTxIdx: number;
}) {
  let sessionEntry = contentMessage.new[sessionRow.sessionID];

  if (!sessionEntry) {
    sessionEntry = {
      after: firstNewTxIdx,
      lastSignature: "WILL_BE_REPLACED" as CojsonInternalTypes.Signature,
      newTransactions: [],
    };
    contentMessage.new[sessionRow.sessionID] = sessionEntry;
  }

  for (const tx of newTxsInSession) {
    sessionEntry.newTransactions.push(tx.tx);
  }

  sessionEntry.lastSignature = signature;
}

export function getDependedOnCoValues(
  header: CojsonInternalTypes.CoValueHeader,
  contentMessage: CojsonInternalTypes.NewContentMessage,
) {
  const id = contentMessage.id;
  const sessionIDs = Object.keys(contentMessage.new) as SessionID[];
  const transactions = Object.values(contentMessage.new).map(
    (entry) => entry.newTransactions,
  );

  return cojsonInternals.getDependedOnCoValuesFromRawData(
    id,
    header,
    sessionIDs,
    transactions,
  );
}
