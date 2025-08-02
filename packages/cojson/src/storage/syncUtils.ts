import { getTransactionSize } from "../coValueContentMessage.js";
import { getDependedOnCoValuesFromRawData } from "../coValueCore/utils.js";
import type {
  CoValueHeader,
  Transaction,
} from "../coValueCore/verifiedState.js";
import type { Signature } from "../crypto/crypto.js";
import type { SessionID } from "../exports.js";
import type { NewContentMessage } from "../sync.js";
import type { StoredSessionRow, TransactionRow } from "./types.js";

export function collectNewTxs({
  newTxsInSession,
  contentMessage,
  sessionRow,
  firstNewTxIdx,
  signature,
}: {
  newTxsInSession: TransactionRow[];
  contentMessage: NewContentMessage;
  sessionRow: StoredSessionRow;
  signature: Signature;
  firstNewTxIdx: number;
}) {
  let sessionEntry = contentMessage.new[sessionRow.sessionID];

  if (!sessionEntry) {
    sessionEntry = {
      after: firstNewTxIdx,
      lastSignature: signature,
      newTransactions: [],
    };
    contentMessage.new[sessionRow.sessionID] = sessionEntry;
  } else {
    sessionEntry.lastSignature = signature;
  }

  for (const tx of newTxsInSession) {
    sessionEntry.newTransactions.push(tx.tx);
  }
}

export function getDependedOnCoValues(
  header: CoValueHeader,
  contentMessage: NewContentMessage,
) {
  const id = contentMessage.id;
  const sessionIDs = Object.keys(contentMessage.new) as SessionID[];
  const transactions = Object.values(contentMessage.new).map(
    (entry) => entry.newTransactions,
  );

  return getDependedOnCoValuesFromRawData(id, header, sessionIDs, transactions);
}

export function getNewTransactionsSize(newTxs: Transaction[]) {
  return newTxs.reduce((sum, tx) => sum + getTransactionSize(tx), 0);
}
