import { getGroupDependentKey } from "../ids.js";
import { RawCoID, SessionID } from "../ids.js";
import { Stringified, parseJSON } from "../jsonStringify.js";
import { JsonValue } from "../jsonValue.js";
import { accountOrAgentIDfromSessionID } from "../typeUtils/accountOrAgentIDfromSessionID.js";
import { isAccountID } from "../typeUtils/isAccountID.js";
import { CoValueHeader, Transaction } from "./verifiedState.js";

export function getDependedOnCoValuesFromRawData(
  id: RawCoID,
  header: CoValueHeader,
  sessions: Iterable<SessionID>,
  transactions: Iterable<Iterable<Transaction>>,
): Set<RawCoID> {
  const deps = new Set<RawCoID>();

  for (const session of sessions) {
    const accountId = accountOrAgentIDfromSessionID(session);

    if (isAccountID(accountId) && accountId !== id) {
      deps.add(accountId);
    }
  }

  if (header.ruleset.type === "group") {
    for (const txs of transactions) {
      for (const tx of txs) {
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
            const key = getGroupDependentKey(change.key);

            if (key && key !== id) {
              deps.add(key);
            }
          }
        }
      }
    }
  }

  if (header.ruleset.type === "ownedByGroup") {
    deps.add(header.ruleset.group);
  }

  return deps;
}

function safeParseChanges(value: Stringified<JsonValue[]>): JsonValue[] {
  try {
    return parseJSON(value);
  } catch (e) {
    return [];
  }
}
