import { AgentID, RawAccountID, RawCoID, SessionID } from "../../exports.js";
import { Stringified, stableStringify } from "../../jsonStringify.js";
import {
  CoValueHeader,
  CoValueState,
  emptyCoValueState,
} from "../../localNode/structure.js";

function getIdFromTestHeader(header: CoValueHeader): RawCoID {
  return `co_z${header.type}${header.uniqueness}${stableStringify(header.ruleset)}`;
}

export function createTestCoMap(groupId: RawCoID | null, uniqueness: string) {
  const header = {
    type: "comap",
    ruleset: groupId
      ? { type: "ownedByGroup", group: groupId }
      : { type: "unsafeAllowAll" },
    meta: null,
    uniqueness: uniqueness,
    createdAt: null,
  } satisfies CoValueHeader;

  const coValueState = emptyCoValueState(getIdFromTestHeader(header));
  coValueState.header = header;

  return coValueState;
}

export function createTestGroup(
  owner: RawAccountID | AgentID,
  uniqueness: string,
) {
  const header = {
    type: "comap",
    ruleset: { type: "group", initialAdmin: owner },
    meta: null,
    uniqueness: uniqueness,
    createdAt: null,
  } satisfies CoValueHeader;

  const coValueState = emptyCoValueState(getIdFromTestHeader(header));
  coValueState.header = header;

  return coValueState;
}

export function addMemberTestTransaction(
  coValue: CoValueState,
  member: RawCoID,
  sessionId: SessionID,
) {
  if (!coValue.sessions[sessionId]) {
    coValue.sessions[sessionId] = {
      id: sessionId,
      transactions: [],
      streamingHash: null,
      lastAvailable: -1,
      lastDepsAvailable: -1,
      lastVerified: -1,
      lastDecrypted: -1,
    };
  }

  const tx = {
    state: "available" as const,
    tx: {
      privacy: "trusting" as const,
      changes: JSON.stringify([
        { op: "set", key: member, value: "reader" },
      ]) as Stringified<string[]>,
      madeAt: Date.now(),
    },
    signature: null,
  };

  coValue.sessions[sessionId].transactions.push(tx);
  coValue.sessions[sessionId].lastAvailable++;
}

export function addParentGroupTestTransaction(
  coValue: CoValueState,
  parent: RawCoID,
  sessionId: SessionID,
) {
  if (!coValue.sessions[sessionId]) {
    coValue.sessions[sessionId] = {
      id: sessionId,
      transactions: [],
      streamingHash: null,
      lastAvailable: -1,
      lastDepsAvailable: -1,
      lastVerified: -1,
      lastDecrypted: -1,
    };
  }

  const tx = {
    state: "available" as const,
    tx: {
      privacy: "trusting" as const,
      changes: JSON.stringify([
        { op: "set", key: `parent_${parent}`, value: "extend" },
      ]) as Stringified<string[]>,
      madeAt: Date.now(),
    },
    signature: null,
  };

  coValue.sessions[sessionId].transactions.push(tx);
  coValue.sessions[sessionId].lastAvailable++;
}
