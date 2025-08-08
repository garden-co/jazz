import { type CryptoProvider, LocalNode } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { WebSocket } from "ws";
import { createWebSocketPeer } from "../createWebSocketPeer";
import { startSyncServer } from "./syncServer";
import { waitFor } from "./utils";

describe("WebSocket Peer Integration", () => {
  let server: Awaited<ReturnType<typeof startSyncServer>>;
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

    if (serverMap === "unavailable") {
      throw new Error("Server map is unavailable");
    }

    expect(serverMap.get("testKey")?.toString()).toBe("testValue");
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

  test("calling terminate on the server should close the connection", async () => {
    const ws = new WebSocket(syncServerUrl);
    let disconnectCalled = false;

    createWebSocketPeer({
      id: "test-client",
      websocket: ws,
      role: "server",
      onClose: () => {
        disconnectCalled = true;
      },
    });

    await waitFor(() => {
      expect(server.wss.clients.size).toBe(1);
    });

    const peerOnServer = server.localNode.syncManager.getPeers()[0];

    for (const client of server.wss.clients) {
      client.terminate();
    }

    await waitFor(() => {
      expect(disconnectCalled).toBe(true);
    });

    expect(peerOnServer?.closed).toBe(true);
  });
});
