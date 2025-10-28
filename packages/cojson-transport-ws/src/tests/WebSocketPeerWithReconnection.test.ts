import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { WebSocketPeerWithReconnection } from "../WebSocketPeerWithReconnection";
import { startSyncServer } from "./syncServer";
import { waitFor } from "./utils";

describe("WebSocketPeerWithReconnection", () => {
  let server: any;
  let syncServerUrl: string;

  beforeEach(async () => {
    const result = await startSyncServer();
    server = result;
    syncServerUrl = result.syncServer;
  });

  afterEach(() => {
    server.close();
  });

  test("should connect successfully to sync server", async () => {
    const addPeer = vi.fn();
    const removePeer = vi.fn();

    const peer = new WebSocketPeerWithReconnection({
      peer: syncServerUrl,
      reconnectionTimeout: 100,
      addPeer,
      removePeer,
    });

    peer.enable();

    // Wait for connection to establish
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(addPeer).toHaveBeenCalledTimes(1);
    expect(removePeer).not.toHaveBeenCalled();

    peer.disable();
  });

  test("should attempt reconnection when server disconnects", async () => {
    const addPeer = vi.fn();
    const removePeer = vi.fn();

    const peer = new WebSocketPeerWithReconnection({
      peer: syncServerUrl,
      reconnectionTimeout: 100,
      addPeer,
      removePeer,
    });

    peer.enable();

    // Wait for initial connection
    await new Promise((resolve) => setTimeout(resolve, 100));

    addPeer.mockClear();

    // Close server to simulate disconnect
    server.close();

    // Wait for disconnect to be detected
    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(removePeer).toHaveBeenCalled();
    expect(peer.reconnectionAttempts).toBeGreaterThan(0);

    // Start server again
    server = await startSyncServer(server.port);

    // Wait for reconnection
    await new Promise((resolve) => setTimeout(resolve, 300));

    expect(addPeer).toHaveBeenCalled();

    peer.disable();
  });

  test("should stop reconnection attempts when disabled", async () => {
    const addPeer = vi.fn();
    const removePeer = vi.fn();

    const peer = new WebSocketPeerWithReconnection({
      peer: syncServerUrl,
      reconnectionTimeout: 100,
      addPeer,
      removePeer,
    });

    peer.enable();

    // Wait for initial connection
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Close server and disable peer
    server.close();
    peer.disable();

    // Wait to ensure no reconnection attempts
    await new Promise((resolve) => setTimeout(resolve, 300));

    expect(addPeer).toHaveBeenCalledTimes(1);
    expect(removePeer).toHaveBeenCalledTimes(1);
    expect(peer.reconnectionAttempts).toBe(0);
  });

  test("should reset reconnection attempts when connection is successful", async () => {
    const addPeer = vi.fn();
    const removePeer = vi.fn();

    const peer = new WebSocketPeerWithReconnection({
      peer: syncServerUrl,
      reconnectionTimeout: 10,
      addPeer,
      removePeer,
    });

    peer.enable();

    // Wait for initial connection
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Close server to trigger reconnection attempts
    server.close();

    // Wait for some reconnection attempts
    await new Promise((resolve) => setTimeout(resolve, 300));

    const previousAttempts = peer.reconnectionAttempts;
    expect(previousAttempts).toBeGreaterThan(0);

    // Start server again
    server = await startSyncServer(server.port);

    // Wait for successful reconnection
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await waitFor(() => expect(peer.reconnectionAttempts).toBe(0));

    peer.disable();
  });

  describe("waitUntilConnected", () => {
    test("should wait until connected before resolving", async () => {
      const addPeer = vi.fn();
      const removePeer = vi.fn();

      const peer = new WebSocketPeerWithReconnection({
        peer: syncServerUrl,
        reconnectionTimeout: 100,
        addPeer,
        removePeer,
      });

      // Start waiting for connection before enabling
      const waitPromise = peer.waitUntilConnected();

      // Enable the peer after a short delay
      setTimeout(() => peer.enable(), 100);

      // Wait for connection to be established
      await waitPromise;

      expect(addPeer).toHaveBeenCalledTimes(1);
      expect(peer.closed).toBe(false);

      peer.disable();
    });

    test("should resolve immediately if already connected", async () => {
      const addPeer = vi.fn();
      const removePeer = vi.fn();

      const peer = new WebSocketPeerWithReconnection({
        peer: syncServerUrl,
        reconnectionTimeout: 100,
        addPeer,
        removePeer,
      });

      // Enable the peer first
      peer.enable();

      // Wait for initial connection
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Now wait for connection again
      const waitPromise = peer.waitUntilConnected();

      // Should resolve immediately since we're already connected
      await waitPromise;

      expect(addPeer).toHaveBeenCalledTimes(1);
      expect(peer.closed).toBe(false);

      peer.disable();
    });

    test("should work when connection is lost and regained", async () => {
      const addPeer = vi.fn();
      const removePeer = vi.fn();

      const peer = new WebSocketPeerWithReconnection({
        peer: syncServerUrl,
        reconnectionTimeout: 100,
        addPeer,
        removePeer,
      });

      peer.enable();

      // Wait for initial connection
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Close server to simulate connection loss
      server.close();

      // Wait for disconnect to be detected
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Start server again
      server = await startSyncServer(server.port);

      // Wait for the waitUntilConnected promise to resolve
      await peer.waitUntilConnected();

      // Verify that we have a new connection
      expect(addPeer).toHaveBeenCalledTimes(3); // Once for initial, once for reconnection
      expect(peer.closed).toBe(false);

      peer.disable();
    });
  });

  describe("subscribe", () => {
    test("should notify subscribers of initial connection state", async () => {
      const addPeer = vi.fn();
      const removePeer = vi.fn();
      const listener = vi.fn();

      const peer = new WebSocketPeerWithReconnection({
        peer: syncServerUrl,
        reconnectionTimeout: 100,
        addPeer,
        removePeer,
      });

      // Subscribe before enabling
      peer.subscribe(listener);

      // Initial state should be disconnected
      expect(listener).toHaveBeenCalledWith(false);

      // Enable the peer
      peer.enable();

      // Wait for connection
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Should notify of connected state
      expect(listener).toHaveBeenCalledWith(true);

      peer.disable();
    });

    test("should notify subscribers of connection state changes", async () => {
      const addPeer = vi.fn();
      const removePeer = vi.fn();
      const listener = vi.fn();

      const peer = new WebSocketPeerWithReconnection({
        peer: syncServerUrl,
        reconnectionTimeout: 100,
        addPeer,
        removePeer,
      });

      peer.enable();
      peer.subscribe(listener);

      // Wait for initial connection
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Should notify of connected state
      expect(listener).toHaveBeenCalledWith(true);

      // Close server to simulate disconnect
      server.close();

      // Wait for disconnect to be detected
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Should notify of disconnected state
      expect(listener).toHaveBeenCalledWith(false);

      peer.disable();
    });

    test("should not notify unsubscribed listeners", async () => {
      const addPeer = vi.fn();
      const removePeer = vi.fn();
      const listener1 = vi.fn();
      const listener2 = vi.fn();

      const peer = new WebSocketPeerWithReconnection({
        peer: syncServerUrl,
        reconnectionTimeout: 100,
        addPeer,
        removePeer,
      });

      peer.enable();
      peer.subscribe(listener1);
      peer.subscribe(listener2);

      // Wait for initial connection
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Both listeners should be notified of connection
      expect(listener1).toHaveBeenCalledWith(true);
      expect(listener2).toHaveBeenCalledWith(true);

      // Unsubscribe listener1
      peer.unsubscribe(listener1);

      listener1.mockClear();
      listener2.mockClear();

      // Close server to simulate disconnect
      server.close();

      // Wait for disconnect to be detected
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Only listener2 should be notified of disconnect
      expect(listener1).not.toHaveBeenCalled();
      expect(listener2).toHaveBeenCalledWith(false);

      peer.disable();
    });

    test("should handle multiple subscribers correctly", async () => {
      const addPeer = vi.fn();
      const removePeer = vi.fn();
      const listeners = Array.from({ length: 3 }, () => vi.fn());

      const peer = new WebSocketPeerWithReconnection({
        peer: syncServerUrl,
        reconnectionTimeout: 100,
        addPeer,
        removePeer,
      });

      peer.enable();

      // Subscribe all listeners
      for (const listener of listeners) {
        peer.subscribe(listener);
      }

      // Wait for initial connection
      await new Promise((resolve) => setTimeout(resolve, 100));

      // All listeners should be notified of connection
      for (const listener of listeners) {
        expect(listener).toHaveBeenCalledWith(true);
      }

      // Close server to simulate disconnect
      server.close();

      // Wait for disconnect to be detected
      await new Promise((resolve) => setTimeout(resolve, 200));

      // All listeners should be notified of disconnect
      for (const listener of listeners) {
        expect(listener).toHaveBeenCalledWith(false);
      }

      peer.disable();
    });
  });
});
