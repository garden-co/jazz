import { getGroupDependentKey } from "../ids.js";
import { RawCoID, SessionID } from "../ids.js";
import { Stringified, parseJSON } from "../jsonStringify.js";
import { JsonValue } from "../jsonValue.js";
import { accountOrAgentIDfromSessionID } from "../typeUtils/accountOrAgentIDfromSessionID.js";
import { isAccountID } from "../typeUtils/isAccountID.js";
import { CoValueHeader, Transaction } from "./verifiedState.js";

export function getDependenciesFromHeader(
  header: CoValueHeader,
  deps: Set<RawCoID>,
): void {
  if (header.ruleset.type === "ownedByGroup") {
    deps.add(header.ruleset.group);
  }

  if (header.meta?.source) {
    deps.add(header.meta.source as RawCoID);
  }
}

export function getDependenciesFromSessions(
  sessions: Iterable<SessionID>,
  deps: Set<RawCoID>,
): void {
  for (const session of sessions) {
    const accountId = accountOrAgentIDfromSessionID(session);

    if (isAccountID(accountId)) {
      deps.add(accountId);
    }
  }
}

export function getDependenciesFromGroupRawTransactions(
  transactions: Iterable<Transaction>,
  deps: Set<RawCoID>,
): void {
  for (const tx of transactions) {
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

        if (key) {
          deps.add(key);
        }
      }
    }
  }
}

export function getDependedOnCoValuesFromRawData(
  id: RawCoID,
  header: CoValueHeader,
  sessions: Iterable<SessionID>,
  transactions: Iterable<Iterable<Transaction>>,
): Set<RawCoID> {
  const deps = new Set<RawCoID>();

  getDependenciesFromHeader(header, deps);
  getDependenciesFromSessions(sessions, deps);

  if (header.ruleset.type === "group") {
    for (const txs of transactions) {
      getDependenciesFromGroupRawTransactions(txs, deps);
    }
  }

  // A value cannot depend on itself
  deps.delete(id);

  return deps;
}

function safeParseChanges(value: Stringified<JsonValue[]>): JsonValue[] {
  try {
    return parseJSON(value);
  } catch (e) {
    return [];
  }
}
