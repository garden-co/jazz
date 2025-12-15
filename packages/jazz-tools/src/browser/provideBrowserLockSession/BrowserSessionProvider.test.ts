// @vitest-environment happy-dom

import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { SessionID } from "cojson";
import { beforeEach, describe, expect, test, vi } from "vitest";
import { BrowserSessionProvider } from "./BrowserSessionProvider.js";
import { SessionIDStorage } from "./SessionIDStorage.js";
import { createJazzTestAccount } from "jazz-tools/testing";
import type { CryptoProvider } from "jazz-tools";

const Crypto = await WasmCrypto.create();

// Mock navigator.locks
interface LockInfo {
  mode: "exclusive" | "shared";
  release: () => void;
}

const mockLocks = new Map<string, LockInfo>();

function isLockAvailable(
  name: string,
  requestedMode: "exclusive" | "shared",
): boolean {
  const existingLock = mockLocks.get(name);
  if (!existingLock) {
    return true; // No existing lock
  }

  // Exclusive locks block everything
  if (existingLock.mode === "exclusive" || requestedMode === "exclusive") {
    return false;
  }

  // Shared locks can coexist with other shared locks
  if (existingLock.mode === "shared" && requestedMode === "shared") {
    return true;
  }

  return false;
}

function createMockLock(
  name: string,
  mode: "exclusive" | "shared",
): {
  lock: { name: string } | null;
  release: () => void;
} {
  if (!isLockAvailable(name, mode)) {
    return { lock: null, release: () => {} };
  }

  // Create new lock
  const lockInfo: LockInfo = {
    mode,
    release: () => {
      mockLocks.delete(name);
    },
  };

  mockLocks.set(name, lockInfo);

  return {
    lock: { name },
    release: lockInfo.release,
  };
}

vi.stubGlobal("navigator", {
  locks: {
    request: vi.fn(
      async (
        name: string,
        options: { mode?: "exclusive" | "shared"; ifAvailable?: boolean },
        callback: (lock: { name: string } | null) => Promise<void> | void,
      ) => {
        const mode = options?.mode || "exclusive";
        const ifAvailable = options?.ifAvailable || false;

        if (ifAvailable) {
          const { lock } = createMockLock(name, mode);
          if (!lock) {
            // Lock not available, call callback with null and return immediately
            await callback(null);
            return;
          }
          // Lock available, call callback with lock
          // The lock is held until the promise returned from callback resolves
          const callbackPromise = callback(lock);
          const result = await callbackPromise;
          // Release lock after callback promise resolves
          mockLocks.get(name)?.release();
          return result;
        }

        // For non-ifAvailable locks, wait until available
        // In a real implementation, this would wait, but for tests we assume immediate availability
        const { lock, release } = createMockLock(name, mode);
        if (!lock) {
          // This shouldn't happen in tests for non-ifAvailable locks
          // But handle it gracefully
          await callback(null);
          return;
        }

        try {
          const callbackPromise = callback(lock);
          const result = await callbackPromise;
          return result;
        } finally {
          release();
        }
      },
    ),
  },
});

describe("BrowserSessionProvider", () => {
  let sessionProvider: BrowserSessionProvider;
  let account: Awaited<ReturnType<typeof createJazzTestAccount>>;

  beforeEach(async () => {
    // Clear localStorage
    localStorage.clear();

    // Clear mock locks
    mockLocks.clear();

    // Create new session provider instance
    sessionProvider = new BrowserSessionProvider();

    // Create test account
    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  describe("acquireSession", () => {
    test("creates new session when none exists", async () => {
      const accountID = account.$jazz.id;

      // Verify no sessions exist
      const existingSessionsBefore =
        SessionIDStorage.getSessionsList(accountID);
      expect(existingSessionsBefore).toEqual([]);

      // Acquire session
      const result = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      // Verify a new session ID is generated
      expect(result.sessionID).toBeDefined();

      // Verify the session is stored in localStorage
      const storedSessions = SessionIDStorage.getSessionsList(accountID);
      expect(storedSessions).toHaveLength(1);
      expect(storedSessions[0]).toBe(result.sessionID);
    });

    test("returns existing session when available", async () => {
      const accountID = account.$jazz.id;
      const existingSessionID = "existing-session-id" as SessionID;

      // Pre-populate localStorage with a session ID
      SessionIDStorage.storeSessionID(accountID, existingSessionID, 0);

      // Verify session exists before calling acquireSession
      const sessionsBefore = SessionIDStorage.getSessionsList(accountID);
      expect(sessionsBefore).toHaveLength(1);
      expect(sessionsBefore[0]).toBe(existingSessionID);

      // Acquire session
      const result = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      // Verify the existing session ID is returned (not a new one)
      expect(result.sessionID).toBe(existingSessionID);

      // Verify no new session is created (same value still in store)
      const sessionsAfter = SessionIDStorage.getSessionsList(accountID);
      expect(sessionsAfter).toHaveLength(1);
      expect(sessionsAfter[0]).toBe(existingSessionID);
      expect(sessionsAfter[0]).toBe(result.sessionID);
    });

    test("handles multiple sessions in list - skips locked sessions and returns next available", async () => {
      const accountID = account.$jazz.id;
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;
      const session3 = "session-3" as SessionID;

      // Pre-populate localStorage with multiple sessions
      SessionIDStorage.storeSessionID(accountID, session1, 0);
      SessionIDStorage.storeSessionID(accountID, session2, 1);
      SessionIDStorage.storeSessionID(accountID, session3, 2);

      // Verify sessions are stored
      const sessionsBefore = SessionIDStorage.getSessionsList(accountID);
      expect(sessionsBefore).toHaveLength(3);

      // Lock the first session (index 0) by manually adding it to mockLocks
      const lockName = `load_session_${session1}`;
      mockLocks.set(lockName, {
        mode: "exclusive",
        release: () => {
          mockLocks.delete(lockName);
        },
      });

      // Acquire session - should skip locked session1 and return session2
      const result = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      // Verify it returned session2 (next available, not session1 which is locked)
      expect(result.sessionID).toBe(session2);

      // Verify the returned session is from the existing list, not a new one
      const allSessions = SessionIDStorage.getSessionsList(accountID);
      expect(allSessions).toContain(result.sessionID);
      expect([session1, session2, session3]).toContain(result.sessionID);

      // Clean up the held lock
      mockLocks.delete(lockName);
    });

    test("creates new session when all existing sessions are locked", async () => {
      const accountID = account.$jazz.id;
      const session1 = "session-1" as SessionID;
      const session2 = "session-2" as SessionID;

      // Pre-populate localStorage with sessions
      SessionIDStorage.storeSessionID(accountID, session1, 0);
      SessionIDStorage.storeSessionID(accountID, session2, 1);

      // Verify sessions are stored
      const sessionsBefore = SessionIDStorage.getSessionsList(accountID);
      expect(sessionsBefore).toHaveLength(2);

      // Lock all existing sessions by manually adding them to mockLocks
      const lock1Name = `load_session_${session1}`;
      const lock2Name = `load_session_${session2}`;
      mockLocks.set(lock1Name, {
        mode: "exclusive",
        release: () => {
          mockLocks.delete(lock1Name);
        },
      });
      mockLocks.set(lock2Name, {
        mode: "exclusive",
        release: () => {
          mockLocks.delete(lock2Name);
        },
      });

      // Acquire session - should create a new one since all existing are locked
      const result = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      // Verify a new session is created (not one of the existing locked ones)
      expect(result.sessionID).not.toBe(session1);
      expect(result.sessionID).not.toBe(session2);

      // Verify the new session is added to localStorage
      const sessionsAfter = SessionIDStorage.getSessionsList(accountID);
      expect(sessionsAfter).toHaveLength(3);
      expect(sessionsAfter).toContain(result.sessionID);

      // Clean up held locks
      mockLocks.delete(lock1Name);
      mockLocks.delete(lock2Name);
    });

    test("releases lock when sessionDone is called", async () => {
      const accountID = account.$jazz.id;

      // Acquire a session
      const result = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      const sessionID = result.sessionID;

      // Call sessionDone to release the lock
      result.sessionDone();

      // Wait for async lock release
      await new Promise((resolve) => setTimeout(resolve, 0));

      // Acquire a session
      const result2 = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      expect(result2.sessionID).toBe(sessionID);
    });
  });

  describe("persistSession", () => {
    test("stores session ID correctly", async () => {
      const accountID = account.$jazz.id;
      const sessionID = "test-session-id" as SessionID;

      // Verify no sessions exist before
      const sessionsBefore = SessionIDStorage.getSessionsList(accountID);
      expect(sessionsBefore).toEqual([]);

      // Persist session
      const result = await sessionProvider.persistSession(accountID, sessionID);

      // Verify the session ID is stored in localStorage at index 0
      const storedSessions = SessionIDStorage.getSessionsList(accountID);
      expect(storedSessions).toHaveLength(1);
      expect(storedSessions[0]).toBe(sessionID);

      // Verify sessionDone callback is provided
      expect(typeof result.sessionDone).toBe("function");
    });

    test("adds to sessions list properly", async () => {
      const accountID = account.$jazz.id;
      const initialSessionID = "initial-session-id" as SessionID;
      const newSessionID = "new-session-id" as SessionID;

      // Pre-populate localStorage with one session (index 0)
      SessionIDStorage.storeSessionID(accountID, initialSessionID, 0);

      // Verify initial session is stored
      const sessionsBefore = SessionIDStorage.getSessionsList(accountID);
      expect(sessionsBefore).toHaveLength(1);
      expect(sessionsBefore[0]).toBe(initialSessionID);

      // Persist a new session ID
      await sessionProvider.persistSession(accountID, newSessionID);

      // Verify the new session is stored at index 1
      const sessionsAfter = SessionIDStorage.getSessionsList(accountID);
      expect(sessionsAfter).toHaveLength(2);
      expect(sessionsAfter[0]).toBe(initialSessionID);
      expect(sessionsAfter[1]).toBe(newSessionID);
    });

    test("locks session when persisting", async () => {
      const accountID = account.$jazz.id;
      const sessionID = "persisted-session-id" as SessionID;

      // Persist session - this should acquire a lock on the session
      const { sessionDone } = await sessionProvider.persistSession(
        accountID,
        sessionID,
      );

      // Verify the session is locked by checking mockLocks directly
      // The lock should be held until sessionDone is called
      const lockName = `load_session_${sessionID}`;
      expect(mockLocks.has(lockName)).toBe(true);

      // Also verify we can't acquire the lock while it's held
      // (This tests the isLockAvailable function)
      expect(isLockAvailable(lockName, "exclusive")).toBe(false);

      // Clean up by releasing the lock
      sessionDone();

      // After releasing, the lock should be removed
      // Note: The lock might be removed asynchronously, so we check after a small delay
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(mockLocks.has(lockName)).toBe(false);
    });

    test("releases lock when sessionDone is called", async () => {
      const accountID = account.$jazz.id;
      const sessionID = "session-to-release" as SessionID;

      // Persist a session
      const { sessionDone } = await sessionProvider.persistSession(
        accountID,
        sessionID,
      );

      // Call sessionDone to release the lock
      sessionDone();

      // Wait for async lock release
      await new Promise((resolve) => setTimeout(resolve, 0));

      // Acquire a session
      const result = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      expect(result.sessionID).toBe(sessionID);
    });
  });
});
