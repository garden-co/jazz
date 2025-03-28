import { randomUUID } from "crypto";
import { createServer } from "http";
import { tmpdir } from "os";
import { dirname, join } from "path";
import { ControlledAgent, LocalNode } from "cojson";
import { SQLiteStorage } from "cojson-storage-sqlite";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { mkdir, unlink } from "fs/promises";
import { WebSocket, WebSocketServer } from "ws";

export type TestSyncServer = Awaited<ReturnType<typeof startSyncServer>>;

export const startSyncServer = async (port?: number, dbName?: string) => {
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
    new ControlledAgent(agentSecret, crypto),
    crypto.newRandomSessionID(agentID),
    crypto,
  );

  const db = join(tmpdir(), `${dbName ?? randomUUID()}.db`);
  await mkdir(dirname(db), { recursive: true });

  const storage = await SQLiteStorage.asPeer({ filename: db });

  localNode.syncManager.addPeer(storage);

  const connections = new Set<WebSocket>();
  let isActive = true;

  wss.on("connection", function connection(ws, req) {
    connections.add(ws);

    if (!isActive) {
      ws.close();
      return;
    }

    const sendPing = () => {
      ws.send(
        JSON.stringify({
          type: "ping",
          time: Date.now(),
          dc: "unknown",
        }),
      );
    };

    // ping/pong for the connection liveness
    const pinging = setInterval(sendPing, 100);

    sendPing(); // Immediately send a ping to the client to signal that the connection is established

    ws.on("close", () => {
      clearInterval(pinging);
      connections.delete(ws);
    });

    const clientId = new Date().toISOString();

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

  server.on("upgrade", function upgrade(req, socket, head) {
    if (req.url !== "/health") {
      wss.handleUpgrade(req, socket, head, function done(ws) {
        wss.emit("connection", ws, req);
      });
    }
  });

  server.listen(port ?? 0);

  port = (server.address() as { port: number }).port;
  const url = `ws://localhost:${port}` as const;

  let closed = false;

  return {
    close: () => {
      if (closed) {
        return;
      }

      closed = true;
      connections.forEach((ws) => ws.close());
      server.close();
    },
    deleteDb: () => unlink(db).catch(() => {}),
    disconnectAllClients: () => {
      connections.forEach((ws) => ws.close());
    },
    setActive: (active: boolean) => {
      if (isActive && !active) {
        // Disconnect all clients when the server is disabled
        connections.forEach((ws) => ws.close());
      }

      isActive = active;
    },
    url,
    port,
    localNode,
  };
};
