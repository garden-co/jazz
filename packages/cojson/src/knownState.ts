import type { RawCoID, SessionID } from "./exports.js";

export type KnownStateSessions = { [sessionID: SessionID]: number };

export type CoValueKnownState = {
  id: RawCoID;
  header: boolean;
  sessions: KnownStateSessions;
};

/**
 * Returns an empty known state for a CoValue, with no header and empty sessions.
 */
export function emptyKnownState(id: RawCoID): CoValueKnownState {
  return {
    id,
    header: false,
    sessions: {},
  };
}

/**
 * Picks the knownState properties from the input object
 */
export function knownStateFrom(input: CoValueKnownState) {
  return {
    id: input.id,
    header: input.header,
    sessions: input.sessions,
  };
}

/**
 * Mutate the target known state by combining the sessions from the source.
 *
 * The function assigns the sessions to the target only when the value in the source is greater.
 */
export function combineKnownStates(
  target: CoValueKnownState,
  source: CoValueKnownState,
): CoValueKnownState {
  combineKnownStateSessions(target.sessions, source.sessions);

  if (source.header && !target.header) {
    target.header = true;
  }

  return target;
}

/**
 * Mutate the target sessions counter by combining the entries from the source.
 *
 * The function assigns the sessions to the target only when the value in the source is greater.
 */
export function combineKnownStateSessions(
  target: KnownStateSessions,
  source: KnownStateSessions,
) {
  for (const [sessionID, count] of Object.entries(source) as [
    SessionID,
    number,
  ][]) {
    const currentCount = target[sessionID] || 0;

    if (count > currentCount) {
      target[sessionID] = count;
    }
  }

  return target;
}

/**
 * Set the session counter for a sessionId in the known state.
 */
export function setSessionCounter(
  knownState: KnownStateSessions,
  sessionId: SessionID,
  value: number,
) {
  knownState[sessionId] = value;
}

/**
 * Update the session counter for a sessionId in the known state.
 *
 * The function assigns the value to the target only when the value in the knownState is less than the provided value.
 */
export function updateSessionCounter(
  knownState: KnownStateSessions,
  sessionId: SessionID,
  value: number,
) {
  knownState[sessionId] = Math.max(knownState[sessionId] || 0, value);
}

/**
 * Efficient cloning of a known state.
 */
export function cloneKnownState(knownState: CoValueKnownState) {
  return {
    id: knownState.id,
    header: knownState.header,
    sessions: { ...knownState.sessions },
  };
}

/**
 * Checks if all the local sessions have the same counters as in remote.
 */
export function isKnownStateSubsetOf(
  local: Record<string, number>,
  remote: Record<string, number>,
) {
  for (const sessionId of Object.keys(local)) {
    if (local[sessionId] !== remote[sessionId]) {
      return false;
    }
  }

  return true;
}

/**
 * Returns the record with the sessions that need to be sent to the target
 */
export function getKnownStateToSend(
  source: Record<string, number>,
  target: Record<string, number>,
) {
  const toSend: Record<string, number> = {};
  for (const [sessionId, sourceCount] of Object.entries(source) as [
    SessionID,
    number,
  ][]) {
    const targetCount = target[sessionId] ?? 0;
    if (sourceCount > targetCount) {
      toSend[sessionId] = sourceCount - targetCount;
    }
  }
  return toSend;
}
