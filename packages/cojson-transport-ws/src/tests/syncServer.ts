import { createServer } from "http";
import { ControlledAgent, LocalNode, WasmCrypto } from "cojson";
import { WebSocket, WebSocketServer } from "ws";
import { createWebSocketPeer } from "../createWebSocketPeer";

export const startSyncServer = async (port?: number) => {
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

  const connections = new Set<WebSocket>();

  wss.on("connection", function connection(ws, req) {
    connections.add(ws);

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
  const syncServer = `ws://localhost:${port}`;

  return {
    close: () => {
      connections.forEach((ws) => ws.close());
      server.close();
    },
    syncServer,
    port,
    localNode,
  };
};
