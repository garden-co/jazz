import { describe, expect, test } from "vitest";
import { PeerKnownState } from "../coValueCore/PeerKnownState.js";
import type { CoValueKnownState } from "../knownState.js";
import { cloneKnownState } from "../knownState.js";
import { RawCoID, SessionID } from "../ids.js";
import type { PeerID } from "../sync.js";

describe("PeerKnownState", () => {
  const testId = "co_ztest123" as RawCoID;
  const testPeerId = "peer123" as PeerID;
  const session1 = "session1_session_z123" as SessionID;
  const session2 = "session2_session_z456" as SessionID;

  describe("constructor", () => {
    test("should initialize with correct peerId and empty knownState", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      expect(peerKnownState.peerId).toBe(testPeerId);
      expect(peerKnownState.value()).toEqual({
        id: testId,
        header: false,
        sessions: {},
      });
      expect(peerKnownState.optimisticValue()).toEqual({
        id: testId,
        header: false,
        sessions: {},
      });
    });

    test("should maintain knownState reference", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const initialKnownState = peerKnownState.value();

      // Verify that the reference is preserved by checking that modifications
      // to the returned object affect subsequent calls to value()
      const state1 = peerKnownState.value();
      const state2 = peerKnownState.value();
      expect(state1).toBe(state2); // Same reference
      expect(state1).toBe(initialKnownState); // Same as initial reference
    });
  });

  describe("updateHeader", () => {
    test("should update header in knownState when no optimistic state exists", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const initialKnownState = peerKnownState.value();

      peerKnownState.updateHeader(true);

      expect(peerKnownState.value().header).toBe(true);
      expect(peerKnownState.value()).toBe(initialKnownState); // Reference preserved
    });

    test("should update header in both knownState and optimisticKnownState", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      // Create optimistic state first
      peerKnownState.combineOptimisticWith({
        id: testId,
        header: false,
        sessions: { [session1]: 5 },
      });

      const knownStateRef = peerKnownState.value();
      const optimisticStateRef = peerKnownState.optimisticValue();

      peerKnownState.updateHeader(true);

      expect(peerKnownState.value().header).toBe(true);
      expect(peerKnownState.optimisticValue().header).toBe(true);
      expect(peerKnownState.value()).toBe(knownStateRef); // Reference preserved
      expect(peerKnownState.optimisticValue()).toBe(optimisticStateRef); // Reference preserved
    });

    test("should update header to false", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      peerKnownState.updateHeader(true);

      peerKnownState.updateHeader(false);

      expect(peerKnownState.value().header).toBe(false);
      expect(peerKnownState.optimisticValue().header).toBe(false);
    });
  });

  describe("combineWith", () => {
    test("should combine sessions and header correctly", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const initialKnownState = peerKnownState.value();

      const toCombine: CoValueKnownState = {
        id: testId,
        header: true,
        sessions: { [session1]: 5, [session2]: 10 },
      };

      peerKnownState.combineWith(toCombine);

      expect(peerKnownState.value()).toEqual({
        id: testId,
        header: true,
        sessions: { [session1]: 5, [session2]: 10 },
      });
      expect(peerKnownState.value()).toBe(initialKnownState); // Reference preserved
    });

    test("should update existing sessions with higher values", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const initialKnownState = peerKnownState.value();

      // Set initial state
      peerKnownState.combineWith({
        id: testId,
        header: false,
        sessions: { [session1]: 3, [session2]: 5 },
      });

      // Combine with higher values for session1, lower for session2
      peerKnownState.combineWith({
        id: testId,
        header: true,
        sessions: { [session1]: 7, [session2]: 2 },
      });

      expect(peerKnownState.value().sessions[session1]).toBe(7); // Updated to higher value
      expect(peerKnownState.value().sessions[session2]).toBe(5); // Kept higher existing value
      expect(peerKnownState.value()).toBe(initialKnownState); // Reference preserved
    });

    test("should update optimisticKnownState when it exists", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      // Create optimistic state
      peerKnownState.combineOptimisticWith({
        id: testId,
        header: false,
        sessions: { [session1]: 3 },
      });

      const optimisticStateRef = peerKnownState.optimisticValue();

      peerKnownState.combineWith({
        id: testId,
        header: true,
        sessions: { [session2]: 10 },
      });

      expect(peerKnownState.optimisticValue().sessions[session1]).toBe(3); // From optimistic state
      expect(peerKnownState.optimisticValue().sessions[session2]).toBe(10); // Combined
      expect(peerKnownState.optimisticValue().header).toBe(true); // Combined header
      expect(peerKnownState.optimisticValue()).toBe(optimisticStateRef); // Reference preserved
    });
  });

  describe("combineOptimisticWith", () => {
    test("should create optimisticKnownState when none exists", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const initialKnownState = peerKnownState.value();

      const toCombine: CoValueKnownState = {
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      };

      peerKnownState.combineOptimisticWith(toCombine);

      expect(peerKnownState.optimisticValue()).toEqual({
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      });
      // Should be a different object from knownState
      expect(peerKnownState.optimisticValue()).not.toBe(peerKnownState.value());
      // Should not affect the known state
      expect(peerKnownState.value()).toEqual(initialKnownState);
    });

    test("should combine with existing optimisticKnownState", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      // Create initial optimistic state
      peerKnownState.combineOptimisticWith({
        id: testId,
        header: false,
        sessions: { [session1]: 3 },
      });

      const optimisticStateRef = peerKnownState.optimisticValue();

      // Combine with additional state
      peerKnownState.combineOptimisticWith({
        id: testId,
        header: true,
        sessions: { [session2]: 7 },
      });

      expect(peerKnownState.optimisticValue()).toEqual({
        id: testId,
        header: true,
        sessions: { [session1]: 3, [session2]: 7 },
      });
      expect(peerKnownState.optimisticValue()).toBe(optimisticStateRef); // Reference preserved
    });

    test("should preserve knownState when combining optimistic", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const initialKnownState = peerKnownState.value();

      peerKnownState.combineOptimisticWith({
        id: testId,
        header: true,
        sessions: { [session1]: 10 },
      });

      expect(peerKnownState.value()).toBe(initialKnownState); // Reference preserved
      expect(peerKnownState.value().sessions).toEqual({}); // Known state unchanged
    });
  });

  describe("set", () => {
    test("should set knownState with provided CoValueKnownState", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const initialKnownState = peerKnownState.value();

      // Create optimistic state first
      peerKnownState.combineOptimisticWith({
        id: testId,
        header: false,
        sessions: { [session1]: 5 },
      });

      const newState: CoValueKnownState = {
        id: testId,
        header: true,
        sessions: { [session1]: 10, [session2]: 20 },
      };

      peerKnownState.set(newState);

      expect(peerKnownState.value()).toEqual(newState);
      expect(peerKnownState.value()).toBe(initialKnownState); // Reference preserved
      expect(peerKnownState.optimisticValue()).toBe(peerKnownState.value()); // Should return knownState when optimistic is cleared
    });

    test("should handle shallow copy of sessions in set", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const initialKnownState = peerKnownState.value();

      const sessions = { [session1]: 5, [session2]: 10 };
      const newState: CoValueKnownState = {
        id: testId,
        header: true,
        sessions,
      };

      peerKnownState.set(newState);

      expect(peerKnownState.value().sessions).toEqual(sessions);
      expect(peerKnownState.value().sessions).not.toBe(sessions); // Should be a copy
      expect(peerKnownState.value()).toBe(initialKnownState); // Reference preserved
    });

    test("should set empty state when payload is 'empty'", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      // Set some initial state
      peerKnownState.combineWith({
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      });

      // Create optimistic state
      peerKnownState.combineOptimisticWith({
        id: testId,
        header: false,
        sessions: { [session2]: 10 },
      });

      const initialKnownState = peerKnownState.value();
      peerKnownState.set("empty");

      expect(peerKnownState.value()).toEqual({
        id: testId,
        header: false,
        sessions: {},
      });
      expect(peerKnownState.value()).toBe(initialKnownState); // Reference preserved
      expect(peerKnownState.optimisticValue()).toBe(peerKnownState.value()); // Should return knownState when optimistic is cleared
    });

    test("should clear optimisticKnownState on set", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      // Create optimistic state
      peerKnownState.combineOptimisticWith({
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      });

      expect(peerKnownState.optimisticValue()).toBeDefined();
      expect(peerKnownState.optimisticValue()).not.toBe(peerKnownState.value());

      peerKnownState.set("empty");

      expect(peerKnownState.optimisticValue()).toBe(peerKnownState.value()); // Should return knownState when optimistic is cleared
    });
  });

  describe("value", () => {
    test("should return the current knownState", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const state1 = peerKnownState.value();
      const state2 = peerKnownState.value();

      expect(state1).toBe(state2); // Same reference
      expect(state1).toEqual({
        id: testId,
        header: false,
        sessions: {},
      });
    });

    test("should return updated knownState after modifications", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      peerKnownState.combineWith({
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      });

      expect(peerKnownState.value()).toEqual({
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      });
    });
  });

  describe("optimisticValue", () => {
    test("should return knownState when no optimistic state exists", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      expect(peerKnownState.optimisticValue()).toBe(peerKnownState.value());
    });

    test("should return optimisticKnownState when it exists", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      peerKnownState.combineOptimisticWith({
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      });

      const optimisticState = peerKnownState.optimisticValue();
      expect(optimisticState).not.toBe(peerKnownState.value());
      expect(optimisticState).toEqual({
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      });
    });

    test("should maintain reference consistency", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);

      peerKnownState.combineOptimisticWith({
        id: testId,
        header: true,
        sessions: { [session1]: 5 },
      });

      const optimisticState1 = peerKnownState.optimisticValue();
      const optimisticState2 = peerKnownState.optimisticValue();

      expect(optimisticState1).toBe(optimisticState2); // Same reference
    });
  });

  describe("integration scenarios", () => {
    test("should handle complex workflow maintaining references", () => {
      const peerKnownState = new PeerKnownState(testId, testPeerId);
      const originalKnownState = peerKnownState.value();

      // Initial state setup
      peerKnownState.updateHeader(true);
      expect(peerKnownState.value()).toBe(originalKnownState);

      // Combine with some sessions
      peerKnownState.combineWith({
        id: testId,
        header: false,
        sessions: { [session1]: 3, [session2]: 7 },
      });
      expect(peerKnownState.value()).toBe(originalKnownState);

      // Create optimistic state
      peerKnownState.combineOptimisticWith({
        id: testId,
        header: true,
        sessions: { [session2]: 5 },
      });

      const originalOptimisticState = peerKnownState.optimisticValue();

      // Update header should affect both states
      peerKnownState.updateHeader(false);
      expect(peerKnownState.value()).toBe(originalKnownState);
      expect(peerKnownState.optimisticValue()).toBe(originalOptimisticState);

      // Set should clear optimistic and update known
      peerKnownState.set({
        id: testId,
        header: true,
        sessions: { [session1]: 20 },
      });
      expect(peerKnownState.value()).toBe(originalKnownState);
      expect(peerKnownState.optimisticValue()).toBe(peerKnownState.value()); // Should return knownState when optimistic is cleared
    });
  });
});
