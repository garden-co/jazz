import { createServer } from "http";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { LocalNode } from "cojson";
import { SQLiteStorage } from "cojson-storage-sqlite";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WebSocket, WebSocketServer } from "ws";

export const startSyncServer = async ({
  port,
  inMemory,
  db,
}: {
  port: string | undefined;
  inMemory: boolean;
  db: string;
}) => {
  const crypto = await WasmCrypto.create();

  const server = createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200);
      res.end("ok");
    }
  });
  const wss = new WebSocketServer({ noServer: true });

  const agentSecret = crypto.newRandomAgentSecret();
  const agentID = crypto.getAgentID(agentSecret);

  const localNode = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(agentID),
    crypto,
  );

  if (!inMemory) {
    await mkdir(dirname(db), { recursive: true });

    const storage = await SQLiteStorage.asPeer({ filename: db });

    localNode.syncManager.addPeer(storage);
  }

  const aliveSockets = new WeakSet<WebSocket>();

  wss.on("connection", function connection(ws, req) {
    aliveSockets.add(ws);

    ws.on("pong", () => {
      aliveSockets.add(ws);
    });

    const clientAddress =
      (req.headers["x-forwarded-for"] as string | undefined)
        ?.split(",")[0]
        ?.trim() || req.socket.remoteAddress;

    const clientId = clientAddress + "@" + new Date().toISOString();

    localNode.syncManager.addPeer(
      createWebSocketPeer({
        id: clientId,
        role: "client",
        websocket: ws,
        expectPings: false,
        batchingByDefault: false,
        deletePeerStateOnClose: true,
      }),
    );

    ws.on("error", (e) => console.error(`Error on connection ${clientId}:`, e));
  });

  // Used to detect dead connections
  const heartbeatInterval = setInterval(function ping() {
    wss.clients.forEach(function each(ws) {
      if (!aliveSockets.has(ws)) {
        return ws.terminate();
      }

      aliveSockets.delete(ws);
      ws.ping();
    });
  }, 30000);

  // Required for the clients to check the connection with the server
  const jazzPingInterval = setInterval(function ping() {
    const jazzPing = JSON.stringify({
      type: "ping",
      time: Date.now(),
      dc: "unknown",
    });

    wss.clients.forEach(function each(ws) {
      ws.send(jazzPing);
    });
  }, 1500);

  wss.on("close", function close() {
    clearInterval(heartbeatInterval);
    clearInterval(jazzPingInterval);
  });

  server.on("upgrade", function upgrade(req, socket, head) {
    if (req.url !== "/health") {
      wss.handleUpgrade(req, socket, head, function done(ws) {
        wss.emit("connection", ws, req);
      });
    }
  });

  server.on("close", () => {
    localNode.gracefulShutdown();
  });

  server.listen(port ? parseInt(port) : undefined);

  const _close = server.close;

  server.close = () => {
    localNode.gracefulShutdown();

    return _close.call(server);
  };

  return server;
};
