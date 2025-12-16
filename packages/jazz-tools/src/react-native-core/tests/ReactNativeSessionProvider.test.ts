import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { SessionID } from "cojson";
import { beforeEach, describe, expect, test } from "vitest";
import { InMemoryKVStore } from "jazz-tools";
import { KvStoreContext, type KvStore } from "jazz-tools";
import { ReactNativeSessionProvider } from "../ReactNativeSessionProvider.js";
import { createJazzTestAccount } from "jazz-tools/testing";
import type { CryptoProvider } from "jazz-tools";

// Initialize KV store for tests
const kvStore = new InMemoryKVStore() as KvStore;
KvStoreContext.getInstance().initialize(kvStore);

const Crypto = await WasmCrypto.create();

describe("ReactNativeSessionProvider", () => {
  let sessionProvider: ReactNativeSessionProvider;
  let account: Awaited<ReturnType<typeof createJazzTestAccount>>;

  beforeEach(async () => {
    // Clear KV store
    kvStore.clearAll();

    // Create new session provider instance
    sessionProvider = new ReactNativeSessionProvider();

    // Create test account
    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  describe("acquireSession", () => {
    test("creates new session when none exists", async () => {
      const accountID = account.$jazz.id;

      // Verify no session exists
      const existingSessionBefore = await kvStore.get(accountID);
      expect(existingSessionBefore).toBeNull();

      // Acquire session
      const result = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      // Verify a new session ID is generated
      expect(result.sessionID).toBeDefined();

      // Verify the session is stored in KvStore
      const storedSession = await kvStore.get(accountID);
      expect(storedSession).toBeDefined();
      expect(storedSession).toBe(result.sessionID);
    });

    test("returns existing session when one exists", async () => {
      const accountID = account.$jazz.id;
      const existingSessionID = "existing-session-id" as SessionID;

      // Pre-populate KvStore with a session ID
      await kvStore.set(accountID, existingSessionID);

      // Verify session exists before calling acquireSession
      const sessionBefore = await kvStore.get(accountID);
      expect(sessionBefore).toBe(existingSessionID);

      // Acquire session
      const result = await sessionProvider.acquireSession(
        accountID,
        Crypto as CryptoProvider,
      );

      // Verify the existing session ID is returned (not a new one)
      expect(result.sessionID).toBe(existingSessionID);

      // Verify no new session is created (same value still in store)
      const sessionAfter = await kvStore.get(accountID);
      expect(sessionAfter).toBe(existingSessionID);
      expect(sessionAfter).toBe(result.sessionID);
    });
  });

  describe("persistSession", () => {
    test("stores session ID correctly", async () => {
      const accountID = account.$jazz.id;
      const sessionID = "test-session-id" as SessionID;

      // Verify no session exists before
      const sessionBefore = await kvStore.get(accountID);
      expect(sessionBefore).toBeNull();

      // Persist session
      await sessionProvider.persistSession(accountID, sessionID);

      // Verify the session ID is stored in KvStore
      const storedSession = await kvStore.get(accountID);
      expect(storedSession).toBeDefined();

      // Verify the stored value matches the provided session ID
      expect(storedSession).toBe(sessionID);
    });

    test("overwrites existing session", async () => {
      const accountID = account.$jazz.id;
      const initialSessionID = "initial-session-id" as SessionID;
      const newSessionID = "new-session-id" as SessionID;

      // Store an initial session ID
      await kvStore.set(accountID, initialSessionID);

      // Verify initial session is stored
      const sessionBefore = await kvStore.get(accountID);
      expect(sessionBefore).toBe(initialSessionID);

      // Persist a different session ID
      await sessionProvider.persistSession(accountID, newSessionID);

      // Verify the new session ID replaces the old one
      const sessionAfter = await kvStore.get(accountID);
      expect(sessionAfter).toBe(newSessionID);
      expect(sessionAfter).not.toBe(initialSessionID);
    });
  });
});
