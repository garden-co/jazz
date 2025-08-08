import {
  CoValueHeader,
  Transaction,
  VerifiedState,
} from "./coValueCore/verifiedState.js";
import { MAX_RECOMMENDED_TX_SIZE } from "./config.js";
import { Signature } from "./crypto/crypto.js";
import { RawCoID, SessionID } from "./ids.js";
import { getPriorityFromHeader } from "./priority.js";
import { NewContentMessage, emptyKnownState } from "./sync.js";

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
    return baseSize > MAX_RECOMMENDED_TX_SIZE;
  }

  return baseSize + transactionSize > MAX_RECOMMENDED_TX_SIZE;
}

export function knownStateFromContent(content: NewContentMessage) {
  const knownState = emptyKnownState(content.id);

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
