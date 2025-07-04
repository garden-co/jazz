import { ControlledAgent, type CryptoProvider, LocalNode } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { WebSocket } from "ws";
import { createWebSocketPeer } from "../createWebSocketPeer";
import { startSyncServer } from "./syncServer";
import { waitFor } from "./utils";

describe("WebSocket Peer Integration", () => {
  let server: any;
  let syncServerUrl: string;
  let crypto: CryptoProvider;

  beforeEach(async () => {
    crypto = await WasmCrypto.create();
    const result = await startSyncServer();
    server = result;
    syncServerUrl = result.syncServer;
  });

  afterEach(() => {
    server.close();
  });

  test("should establish connection between client and server nodes", async () => {
    // Create client node
    const clientAgent = crypto.newRandomAgentSecret();
    const clientNode = new LocalNode(
      clientAgent,
      crypto.newRandomSessionID(crypto.getAgentID(clientAgent)),
      crypto,
    );

    // Create WebSocket connection
    const ws = new WebSocket(syncServerUrl);

    // Track connection success
    let connectionEstablished = false;

    // Create peer and add to client node
    const peer = createWebSocketPeer({
      id: "test-client",
      websocket: ws,
      role: "server",
      onSuccess: () => {
        connectionEstablished = true;
      },
    });

    clientNode.syncManager.addPeer(peer);

    // Wait for connection to establish
    await new Promise<void>((resolve) => {
      const checkConnection = setInterval(() => {
        if (connectionEstablished) {
          clearInterval(checkConnection);
          resolve();
        }
      }, 100);
    });

    expect(connectionEstablished).toBe(true);
    expect(clientNode.syncManager.getPeers()).toHaveLength(1);
  });

  test("should sync data between nodes through WebSocket connection", async () => {
    const clientAgent = crypto.newRandomAgentSecret();
    const clientNode = new LocalNode(
      clientAgent,
      crypto.newRandomSessionID(crypto.getAgentID(clientAgent)),
      crypto,
    );

    const ws = new WebSocket(syncServerUrl);

    const peer = createWebSocketPeer({
      id: "test-client",
      websocket: ws,
      role: "server",
    });

    clientNode.syncManager.addPeer(peer);

    // Create a test group
    const group = clientNode.createGroup();
    const map = group.createMap();
    map.set("testKey", "testValue", "trusting");

    // Wait for sync
    await map.core.waitForSync();

    // Verify data reached the server
    const serverNode = server.localNode;
    const serverMap = await serverNode.load(map.id);

    expect(serverMap.get("testKey")).toBe("testValue");
  });

  test("should handle disconnection and cleanup", async () => {
    const clientAgent = crypto.newRandomAgentSecret();
    const clientNode = new LocalNode(
      clientAgent,
      crypto.newRandomSessionID(crypto.getAgentID(clientAgent)),
      crypto,
    );

    const ws = new WebSocket(syncServerUrl);
    let disconnectCalled = false;

    const peer = createWebSocketPeer({
      id: "test-client",
      websocket: ws,
      role: "server",
      onClose: () => {
        disconnectCalled = true;
      },
    });

    clientNode.syncManager.addPeer(peer);

    // Wait for connection to establish
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Close the server
    server.close();

    // Wait for disconnect handling
    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(disconnectCalled).toBe(true);
    expect(ws.readyState).toBe(WebSocket.CLOSED);
  });

  test("should trigger a timeout if the server does not respond", async () => {
    const clientAgent = crypto.newRandomAgentSecret();
    const clientNode = new LocalNode(
      clientAgent,
      crypto.newRandomSessionID(crypto.getAgentID(clientAgent)),
      crypto,
    );

    const ws = new WebSocket(syncServerUrl);
    let disconnectCalled = false;

    const peer = createWebSocketPeer({
      id: "test-client",
      websocket: ws,
      role: "server",
      pingTimeout: 5,
      onClose: () => {
        disconnectCalled = true;
      },
    });

    clientNode.syncManager.addPeer(peer);

    // Wait for connection to establish and the timeout to kick in
    await waitFor(() => {
      expect(disconnectCalled).toBe(true);
    });

    expect(ws.readyState).toBe(WebSocket.CLOSED);
  });
});
