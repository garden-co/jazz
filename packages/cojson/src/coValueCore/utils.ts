import { CoValueCore } from "../exports.js";
import { getGroupDependentKey } from "../ids.js";
import { RawCoID, SessionID } from "../ids.js";
import { Stringified, parseJSON } from "../jsonStringify.js";
import { JsonValue } from "../jsonValue.js";
import { NewContentMessage } from "../sync.js";
import { accountOrAgentIDfromSessionID } from "../typeUtils/accountOrAgentIDfromSessionID.js";
import { isAccountID } from "../typeUtils/isAccountID.js";
import { CoValueHeader, Transaction } from "./verifiedState.js";

export function getDependenciesFromHeader(
  header: CoValueHeader,
  deps = new Set<RawCoID>(),
): Set<RawCoID> {
  if (header.ruleset.type === "ownedByGroup") {
    deps.add(header.ruleset.group);
  }

  if (header.meta?.source) {
    deps.add(header.meta.source as RawCoID);
  }

  return deps;
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
  deps = new Set<RawCoID>(),
): Set<RawCoID> {
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

  return deps;
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

  deps.delete(id);

  return deps;
}

export function getDependenciesFromContentMessage(
  coValue: CoValueCore,
  contentMessage: NewContentMessage,
) {
  const deps = new Set<RawCoID>();

  if (contentMessage.header) {
    getDependenciesFromHeader(contentMessage.header, deps);
  }

  const sessions = Object.keys(contentMessage.new) as SessionID[];
  getDependenciesFromSessions(sessions, deps);

  const header = coValue.verified?.header ?? contentMessage.header;

  if (header?.ruleset.type === "group") {
    for (const { newTransactions } of Object.values(contentMessage.new)) {
      getDependenciesFromGroupRawTransactions(newTransactions, deps);
    }
  }

  deps.delete(coValue.id);

  return deps;
}

function safeParseChanges(value: Stringified<JsonValue[]>): JsonValue[] {
  try {
    return parseJSON(value);
  } catch (e) {
    return [];
  }
}
