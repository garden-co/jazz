import {
  CojsonInternalTypes,
  JsonValue,
  SessionID,
  Stringified,
  cojsonInternals,
} from "cojson";
import { StoredCoValueRow, StoredSessionRow, TransactionRow } from "./types.js";

export function collectNewTxs({
  newTxsInSession,
  newDataMessages,
  sessionRow,
  firstNewTxIdx,
}: {
  newTxsInSession: TransactionRow[];
  newDataMessages: CojsonInternalTypes.DataMessage[];
  sessionRow: StoredSessionRow;
  firstNewTxIdx: number;
}) {
  for (const tx of newTxsInSession) {
    let sessionEntry =
      newDataMessages[newDataMessages.length - 1]!.new[sessionRow.sessionID];
    if (!sessionEntry) {
      sessionEntry = {
        after: firstNewTxIdx,
        lastSignature: "WILL_BE_REPLACED" as CojsonInternalTypes.Signature,
        newTransactions: [],
      };
      newDataMessages[newDataMessages.length - 1]!.new[sessionRow.sessionID] =
        sessionEntry;
    }

    sessionEntry.newTransactions.push(tx.tx);
    sessionEntry.lastSignature = sessionRow.lastSignature;
  }
}

export function getDependedOnCoValues({
  coValueRow,
  newDataMessages,
}: {
  coValueRow: StoredCoValueRow;
  newDataMessages: CojsonInternalTypes.DataMessage[];
}) {
  return coValueRow.header.ruleset.type === "group"
    ? getGroupDependedOnCoValues(newDataMessages)
    : coValueRow.header.ruleset.type === "ownedByGroup"
      ? getOwnedByGroupDependedOnCoValues(coValueRow, newDataMessages)
      : [];
}

export function getGroupDependedOnCoValues(
  newDataMessages: CojsonInternalTypes.DataMessage[],
) {
  const keys: CojsonInternalTypes.RawCoID[] = [];

  /**
   * Collect all the signing keys inside the transactions to list all the
   * dependencies required to correctly access the CoValue.
   */
  for (const piece of newDataMessages) {
    for (const sessionEntry of Object.values(piece.new)) {
      for (const tx of sessionEntry.newTransactions) {
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
  }

  return keys;
}

function getOwnedByGroupDependedOnCoValues(
  coValueRow: StoredCoValueRow,
  newDataMessages: CojsonInternalTypes.DataMessage[],
) {
  if (coValueRow.header.ruleset.type !== "ownedByGroup") return [];

  const keys: CojsonInternalTypes.RawCoID[] = [coValueRow.header.ruleset.group];

  /**
   * Collect all the signing keys inside the transactions to list all the
   * dependencies required to correctly access the CoValue.
   */
  for (const piece of newDataMessages) {
    for (const sessionID of Object.keys(piece.new) as SessionID[]) {
      const accountId =
        cojsonInternals.accountOrAgentIDfromSessionID(sessionID);

      if (
        cojsonInternals.isAccountID(accountId) &&
        accountId !== coValueRow.id
      ) {
        keys.push(accountId);
      }
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
