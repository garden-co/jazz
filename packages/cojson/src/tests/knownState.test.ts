import { describe, expect, test } from "vitest";
import {
  emptyKnownState,
  knownStateFrom,
  combineKnownStates,
  combineKnownStateSessions,
  setSessionCounter,
  updateSessionCounter,
  cloneKnownState,
  areLocalSessionsUploaded,
  type CoValueKnownState,
  type KnownStateSessions,
} from "../knownState.js";
import { RawCoID, SessionID } from "../ids.js";

describe("knownState", () => {
  describe("emptyKnownState", () => {
    test("should create an empty known state with the given id", () => {
      const id = "test-id" as RawCoID;
      const result = emptyKnownState(id);

      expect(result).toEqual({
        id: "test-id",
        header: false,
        sessions: {},
      });
    });

    test("should have no sessions", () => {
      const id = "test-id" as RawCoID;
      const result = emptyKnownState(id);

      expect(Object.keys(result.sessions)).toHaveLength(0);
    });

    test("should have header set to false", () => {
      const id = "test-id" as RawCoID;
      const result = emptyKnownState(id);

      expect(result.header).toBe(false);
    });
  });

  describe("knownStateFrom", () => {
    test("should extract known state properties from input", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const input: CoValueKnownState = {
        id,
        header: true,
        sessions: { [session1]: 5 },
      };

      const result = knownStateFrom(input);

      expect(result).toEqual({
        id: "test-id",
        header: true,
        sessions: { [session1]: 5 },
      });
    });

    test("should create a shallow copy of sessions", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const sessions = { [session1]: 5 };
      const input: CoValueKnownState = {
        id,
        header: true,
        sessions,
      };

      const result = knownStateFrom(input);

      expect(result.sessions).toBe(sessions);
    });

    test("should work with empty sessions", () => {
      const id = "test-id" as RawCoID;
      const input = emptyKnownState(id);

      const result = knownStateFrom(input);

      expect(result).toEqual({
        id: "test-id",
        header: false,
        sessions: {},
      });
    });
  });

  describe("combineKnownStates", () => {
    test("should combine sessions from source to target", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const target: CoValueKnownState = {
        id,
        header: false,
        sessions: { [session1]: 3 },
      };
      const source: CoValueKnownState = {
        id,
        header: false,
        sessions: { [session2]: 7 },
      };

      const result = combineKnownStates(target, source);

      expect(result.sessions).toEqual({
        [session1]: 3,
        [session2]: 7,
      });
    });

    test("should update target when source has higher counter", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const target: CoValueKnownState = {
        id,
        header: false,
        sessions: { [session1]: 3 },
      };
      const source: CoValueKnownState = {
        id,
        header: false,
        sessions: { [session1]: 7 },
      };

      combineKnownStates(target, source);

      expect(target.sessions[session1]).toBe(7);
    });

    test("should not update target when source has lower counter", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const target: CoValueKnownState = {
        id,
        header: false,
        sessions: { [session1]: 10 },
      };
      const source: CoValueKnownState = {
        id,
        header: false,
        sessions: { [session1]: 5 },
      };

      combineKnownStates(target, source);

      expect(target.sessions[session1]).toBe(10);
    });

    test("should set header to true when source has header true", () => {
      const id = "test-id" as RawCoID;
      const target: CoValueKnownState = {
        id,
        header: false,
        sessions: {},
      };
      const source: CoValueKnownState = {
        id,
        header: true,
        sessions: {},
      };

      combineKnownStates(target, source);

      expect(target.header).toBe(true);
    });

    test("should not change header when both have header true", () => {
      const id = "test-id" as RawCoID;
      const target: CoValueKnownState = {
        id,
        header: true,
        sessions: {},
      };
      const source: CoValueKnownState = {
        id,
        header: true,
        sessions: {},
      };

      combineKnownStates(target, source);

      expect(target.header).toBe(true);
    });

    test("should not change header when source has header false", () => {
      const id = "test-id" as RawCoID;
      const target: CoValueKnownState = {
        id,
        header: true,
        sessions: {},
      };
      const source: CoValueKnownState = {
        id,
        header: false,
        sessions: {},
      };

      combineKnownStates(target, source);

      expect(target.header).toBe(true);
    });

    test("should return the target object", () => {
      const id = "test-id" as RawCoID;
      const target: CoValueKnownState = {
        id,
        header: false,
        sessions: {},
      };
      const source: CoValueKnownState = {
        id,
        header: false,
        sessions: {},
      };

      const result = combineKnownStates(target, source);

      expect(result).toBe(target);
    });

    test("should mutate the target object", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const target: CoValueKnownState = {
        id,
        header: false,
        sessions: {},
      };
      const source: CoValueKnownState = {
        id,
        header: true,
        sessions: { [session1]: 5 },
      };

      combineKnownStates(target, source);

      expect(target.header).toBe(true);
      expect(target.sessions[session1]).toBe(5);
    });
  });

  describe("combineKnownStateSessions", () => {
    test("should add new sessions from source to target", () => {
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const target: KnownStateSessions = { [session1]: 3 };
      const source: KnownStateSessions = { [session2]: 7 };

      combineKnownStateSessions(target, source);

      expect(target).toEqual({
        [session1]: 3,
        [session2]: 7,
      });
    });

    test("should update session when source has higher counter", () => {
      const session1 = "session-1" as SessionID;
      const target: KnownStateSessions = { [session1]: 3 };
      const source: KnownStateSessions = { [session1]: 7 };

      combineKnownStateSessions(target, source);

      expect(target[session1]).toBe(7);
    });

    test("should not update session when source has lower counter", () => {
      const session1 = "session-1" as SessionID;
      const target: KnownStateSessions = { [session1]: 10 };
      const source: KnownStateSessions = { [session1]: 5 };

      combineKnownStateSessions(target, source);

      expect(target[session1]).toBe(10);
    });

    test("should add session when target has no counter for that session", () => {
      const session1 = "session-1" as SessionID;
      const target: KnownStateSessions = {};
      const source: KnownStateSessions = { [session1]: 5 };

      combineKnownStateSessions(target, source);

      expect(target[session1]).toBe(5);
    });

    test("should handle empty source", () => {
      const session1 = "session-1" as SessionID;
      const target: KnownStateSessions = { [session1]: 3 };
      const source: KnownStateSessions = {};

      combineKnownStateSessions(target, source);

      expect(target).toEqual({ [session1]: 3 });
    });

    test("should handle empty target", () => {
      const session1 = "session-1" as SessionID;
      const target: KnownStateSessions = {};
      const source: KnownStateSessions = { [session1]: 5 };

      combineKnownStateSessions(target, source);

      expect(target).toEqual({ [session1]: 5 });
    });

    test("should handle multiple sessions", () => {
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const session3 = "session-3" as SessionID;
      const target: KnownStateSessions = {
        [session1]: 5,
        [session2]: 10,
      };
      const source: KnownStateSessions = {
        [session1]: 3,
        [session2]: 15,
        [session3]: 8,
      };

      combineKnownStateSessions(target, source);

      expect(target).toEqual({
        [session1]: 5,
        [session2]: 15,
        [session3]: 8,
      });
    });

    test("should return the target object", () => {
      const session1 = "session-1" as SessionID;
      const target: KnownStateSessions = { [session1]: 3 };
      const source: KnownStateSessions = { [session1]: 7 };

      const result = combineKnownStateSessions(target, source);

      expect(result).toBe(target);
    });
  });

  describe("setSessionCounter", () => {
    test("should set counter for a new session", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = {};

      setSessionCounter(knownState, session1, 5);

      expect(knownState[session1]).toBe(5);
    });

    test("should update counter for an existing session", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = { [session1]: 3 };

      setSessionCounter(knownState, session1, 10);

      expect(knownState[session1]).toBe(10);
    });

    test("should allow setting counter to 0", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = { [session1]: 5 };

      setSessionCounter(knownState, session1, 0);

      expect(knownState[session1]).toBe(0);
    });

    test("should allow setting counter to a lower value", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = { [session1]: 10 };

      setSessionCounter(knownState, session1, 3);

      expect(knownState[session1]).toBe(3);
    });

    test("should mutate the input object", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = {};

      setSessionCounter(knownState, session1, 5);

      expect(knownState).toEqual({ [session1]: 5 });
    });
  });

  describe("updateSessionCounter", () => {
    test("should set counter for a new session", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = {};

      updateSessionCounter(knownState, session1, 5);

      expect(knownState[session1]).toBe(5);
    });

    test("should update counter when new value is higher", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = { [session1]: 3 };

      updateSessionCounter(knownState, session1, 10);

      expect(knownState[session1]).toBe(10);
    });

    test("should not update counter when new value is lower", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = { [session1]: 10 };

      updateSessionCounter(knownState, session1, 3);

      expect(knownState[session1]).toBe(10);
    });

    test("should not update counter when new value is equal", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = { [session1]: 5 };

      updateSessionCounter(knownState, session1, 5);

      expect(knownState[session1]).toBe(5);
    });

    test("should handle zero value for existing session", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = { [session1]: 5 };

      updateSessionCounter(knownState, session1, 0);

      expect(knownState[session1]).toBe(5);
    });

    test("should set to 0 when counter does not exist and value is 0", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = {};

      updateSessionCounter(knownState, session1, 0);

      expect(knownState[session1]).toBe(0);
    });

    test("should mutate the input object", () => {
      const session1 = "session-1" as SessionID;
      const knownState: KnownStateSessions = {};

      updateSessionCounter(knownState, session1, 5);

      expect(knownState).toEqual({ [session1]: 5 });
    });
  });

  describe("cloneKnownState", () => {
    test("should create a copy with the same values", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const original: CoValueKnownState = {
        id,
        header: true,
        sessions: { [session1]: 5 },
      };

      const cloned = cloneKnownState(original);

      expect(cloned).toEqual(original);
    });

    test("should create a new object", () => {
      const id = "test-id" as RawCoID;
      const original: CoValueKnownState = {
        id,
        header: true,
        sessions: {},
      };

      const cloned = cloneKnownState(original);

      expect(cloned).not.toBe(original);
    });

    test("should create a new sessions object", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const original: CoValueKnownState = {
        id,
        header: true,
        sessions: { [session1]: 5 },
      };

      const cloned = cloneKnownState(original);

      expect(cloned.sessions).not.toBe(original.sessions);
    });

    test("should not affect original when modifying clone", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const original: CoValueKnownState = {
        id,
        header: false,
        sessions: { [session1]: 5 },
      };

      const cloned = cloneKnownState(original);
      cloned.header = true;
      cloned.sessions[session2] = 10;

      expect(original.header).toBe(false);
      expect(original.sessions[session2]).toBeUndefined();
    });

    test("should work with empty sessions", () => {
      const id = "test-id" as RawCoID;
      const original = emptyKnownState(id);

      const cloned = cloneKnownState(original);

      expect(cloned).toEqual(original);
      expect(cloned).not.toBe(original);
    });

    test("should work with multiple sessions", () => {
      const id = "test-id" as RawCoID;
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const session3 = "session-3" as SessionID;
      const original: CoValueKnownState = {
        id,
        header: true,
        sessions: {
          [session1]: 5,
          [session2]: 10,
          [session3]: 15,
        },
      };

      const cloned = cloneKnownState(original);

      expect(cloned).toEqual(original);
      expect(cloned.sessions).not.toBe(original.sessions);
    });
  });

  describe("areLocalSessionsUploaded", () => {
    test("should return true when all counters match", () => {
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const from = { [session1]: 5, [session2]: 10 };
      const to = { [session1]: 5, [session2]: 10 };

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(true);
    });

    test("should return false when counter differs", () => {
      const session1 = "session-1" as SessionID;
      const from = { [session1]: 5 };
      const to = { [session1]: 3 };

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(false);
    });

    test("should return false when session is missing in to", () => {
      const session1 = "session-1" as SessionID;
      const from = { [session1]: 5 };
      const to = {};

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(false);
    });

    test("should return true when from is empty", () => {
      const session1 = "session-1" as SessionID;
      const from = {};
      const to = { [session1]: 5 };

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(true);
    });

    test("should return true when both are empty", () => {
      const from = {};
      const to = {};

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(true);
    });

    test("should handle multiple sessions", () => {
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const session3 = "session-3" as SessionID;
      const from = {
        [session1]: 5,
        [session2]: 10,
        [session3]: 15,
      };
      const to = {
        [session1]: 5,
        [session2]: 10,
        [session3]: 15,
      };

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(true);
    });

    test("should return false if any session counter differs", () => {
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const session3 = "session-3" as SessionID;
      const from = {
        [session1]: 5,
        [session2]: 10,
        [session3]: 15,
      };
      const to = {
        [session1]: 5,
        [session2]: 8,
        [session3]: 15,
      };

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(false);
    });

    test("should not check sessions in to that are not in from", () => {
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const from = { [session1]: 5 };
      const to = {
        [session1]: 5,
        [session2]: 10,
      };

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(true);
    });

    test("should return false when counter in to is higher", () => {
      const session1 = "session-1" as SessionID;
      const from = { [session1]: 5 };
      const to = { [session1]: 10 };

      const result = areLocalSessionsUploaded(from, to);

      expect(result).toBe(false);
    });
  });
});
