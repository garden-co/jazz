// @vitest-environment happy-dom

import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  AuthSecretStorage,
  BrowserAnonymousAuth,
  createJazzBrowserContext,
} from "../index";
import "fake-indexeddb/auto";
import { cojsonInternals } from "cojson";
import { TestJSCrypto, createJazzTestAccount } from "jazz-tools/testing";
import { createWebSocketPeerWithReconnection } from "../createWebSocketPeerWithReconnection";

const crypto = await TestJSCrypto.create();
const account = await createJazzTestAccount();
const syncServer = account._raw.core.node;

// Mock navigator.locks API
Object.defineProperty(navigator, "locks", {
  value: {
    request: vi.fn().mockImplementation(async (name, options, callback) => {
      // Simulate lock acquisition and callback execution
      const lock = { name };
      return callback(lock);
    }),
  },
  configurable: true,
});

vi.mock("../createWebSocketPeerWithReconnection", () => ({
  createWebSocketPeerWithReconnection: vi.fn(),
}));

describe("createJazzBrowserContext", () => {
  let mockWebSocketPeer: any;

  beforeEach(async () => {
    // Reset mocks and IndexedDB before each test
    vi.clearAllMocks();

    AuthSecretStorage.clear();

    // Clear all IndexedDB databases
    const databases = await indexedDB.databases();
    await Promise.all(
      databases.map(({ name }) =>
        name ? indexedDB.deleteDatabase(name) : Promise.resolve(),
      ),
    );

    const [aPeer, bPeer] = cojsonInternals.connectedPeers("a", "b", {
      peer1role: "client",
      peer2role: "server",
    });

    syncServer.syncManager.addPeer(aPeer);

    // Setup mock for WebSocket peer
    mockWebSocketPeer = {
      peer: bPeer,
      done: vi.fn(),
    };

    vi.mocked(createWebSocketPeerWithReconnection).mockReturnValue(
      mockWebSocketPeer,
    );
  });

  describe("toggleNetwork", () => {
    it("should initialize with network enabled by default", async () => {
      const context = await createJazzBrowserContext({
        peer: "wss://test.com",
        storage: "indexedDB",
        guest: false,
        crypto,
      });

      expect(createWebSocketPeerWithReconnection).toHaveBeenCalledTimes(1);

      context.done();
    });

    it("should initialize with network disabled when localOnly is true", async () => {
      const context = await createJazzBrowserContext({
        peer: "wss://test.com",
        storage: "indexedDB",
        guest: false,
        localOnly: true,
        crypto,
      });

      expect(createWebSocketPeerWithReconnection).not.toHaveBeenCalled();
      context.done();
    });

    it("should enable network when toggled on", async () => {
      const context = await createJazzBrowserContext({
        peer: "wss://test.com",
        storage: "indexedDB",
        guest: false,
        localOnly: true,
        crypto,
      });

      context.toggleNetwork(true);

      expect(createWebSocketPeerWithReconnection).toHaveBeenCalledTimes(1);
    });

    it("should disable network when toggled off", async () => {
      const context = await createJazzBrowserContext({
        peer: "wss://test.com",
        storage: "indexedDB",
        guest: false,
        crypto,
      });

      context.toggleNetwork(false);

      expect(mockWebSocketPeer.done).toHaveBeenCalledTimes(1);
    });

    it("should sync with the server when network is enabled", async () => {
      const context = await createJazzBrowserContext({
        peer: "wss://test.com",
        storage: "indexedDB",
        guest: false,
        localOnly: true,
        crypto,
      });

      if (!("me" in context)) {
        throw new Error("me not found");
      }

      const map = context.me._raw.createMap();

      context.toggleNetwork(true);

      await context.me.waitForAllCoValuesSync();

      const value = await syncServer.load(map.id);

      expect(value).not.toBe("unavailable");
    });

    it("should run the provided auth method", async () => {
      const driver = {
        onSignedIn: vi.fn(),
        onError: vi.fn(),
      } satisfies BrowserAnonymousAuth.Driver;
      const auth = new BrowserAnonymousAuth("Anonymous User", driver);

      await createJazzBrowserContext({
        peer: "wss://test.com",
        storage: "indexedDB",
        guest: false,
        crypto,
        auth,
      });

      expect(driver.onSignedIn).toHaveBeenCalled();
    });
  });
});
