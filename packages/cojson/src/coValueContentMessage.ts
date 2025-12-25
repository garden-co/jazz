import { CoValueHeader, Transaction } from "./coValueCore/verifiedState.js";
import { TRANSACTION_CONFIG } from "./config.js";
import { Signature } from "./crypto/crypto.js";
import { RawCoID, SessionID } from "./ids.js";
import { JsonValue } from "./jsonValue.js";
import { CoValueKnownState, emptyKnownState } from "./knownState.js";
import { getPriorityFromHeader } from "./priority.js";
import { NewContentMessage, SessionNewContent } from "./sync.js";

export function createContentMessage(
  id: RawCoID,
  header: CoValueHeader,
  includeHeader = true,
): NewContentMessage {
  return {
    action: "content",
    id,
    header: includeHeader ? header : undefined,
    priority: getPriorityFromHeader(header),
    new: {},
  };
}

export function addTransactionToContentMessage(
  content: NewContentMessage,
  transaction: Transaction,
  sessionID: SessionID,
  signature: Signature,
  txIdx: number,
) {
  const sessionContent = content.new[sessionID];

  if (sessionContent) {
    sessionContent.newTransactions.push(transaction);
    sessionContent.lastSignature = signature;
  } else {
    content.new[sessionID] = {
      after: txIdx,
      newTransactions: [transaction],
      lastSignature: signature,
    };
  }
}

export function getTransactionSize(transaction: Transaction) {
  return transaction.privacy === "private"
    ? transaction.encryptedChanges.length
    : transaction.changes.length;
}

export function exceedsRecommendedSize(
  baseSize: number,
  transactionSize?: number,
) {
  if (transactionSize === undefined) {
    return baseSize > TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;
  }

  return (
    baseSize + transactionSize > TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE
  );
}

export function knownStateFromContent(content: NewContentMessage) {
  const knownState = emptyKnownState(content.id);
  knownState.header = Boolean(content.header);

  for (const [sessionID, session] of Object.entries(content.new)) {
    knownState.sessions[sessionID as SessionID] =
      session.after + session.newTransactions.length;
  }

  return knownState;
}

export function getContentMessageSize(msg: NewContentMessage) {
  return Object.values(msg.new).reduce((acc, sessionNewContent) => {
    return (
      acc +
      sessionNewContent.newTransactions.reduce((acc, tx) => {
        return acc + getTransactionSize(tx);
      }, 0)
    );
  }, 0);
}

export function getContenDebugInfo(msg: NewContentMessage) {
  return Object.entries(msg.new).map(
    ([sessionID, sessionNewContent]) =>
      `Session: ${sessionID} After: ${sessionNewContent.after} New: ${sessionNewContent.newTransactions.length}`,
  );
}

export function getNewTransactionsFromContentMessage(
  content: SessionNewContent,
  knownState: CoValueKnownState,
  sessionID: SessionID,
) {
  const ourKnownTxIdx = knownState.sessions[sessionID] ?? 0;
  const theirFirstNewTxIdx = content.after;

  if (ourKnownTxIdx < theirFirstNewTxIdx) {
    // Flagging invalid state assumption by returning undefined
    // not throwing an error or return an error object for performance reasons
    return undefined;
  }

  const alreadyKnownOffset = ourKnownTxIdx - theirFirstNewTxIdx;

  const newTransactions = content.newTransactions.slice(alreadyKnownOffset);

  return newTransactions;
}

export function getSessionEntriesFromContentMessage(
  content: NewContentMessage,
) {
  return Object.entries(content.new) as [SessionID, SessionNewContent][];
}
