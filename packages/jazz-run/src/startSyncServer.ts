import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { CryptoProvider, LocalNode } from "cojson";
import { getBetterSqliteStorage } from "cojson-storage-sqlite";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WebSocketServer } from "ws";
import { type SyncServer } from "./types.js";

export const startSyncServer = async ({
  host,
  port,
  inMemory,
  db,
  crypto,
  middleware,
  enableFullStorageReconciliation = false,
}: {
  host: string | undefined;
  port: string | undefined;
  inMemory: boolean;
  db: string;
  crypto?: CryptoProvider;
  middleware?: (req: IncomingMessage, res: ServerResponse) => boolean;
  enableFullStorageReconciliation?: boolean;
}): Promise<SyncServer> => {
  crypto ??= await WasmCrypto.create();

  const server = createServer((req, res) => {
    if (middleware?.(req, res)) {
      return;
    }
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
    undefined,
    enableFullStorageReconciliation,
  );

  if (!inMemory) {
    await mkdir(dirname(db), { recursive: true });

    const storage = getBetterSqliteStorage(db);

    localNode.setStorage(storage);
  }

  localNode.enableGarbageCollector();

  wss.on("connection", function connection(ws, req) {
    // ping/pong for the connection liveness
    const pinging = setInterval(() => {
      ws.send(
        JSON.stringify({
          type: "ping",
          time: Date.now(),
          dc: "unknown",
        }),
      );
    }, 1500);

    ws.on("close", () => {
      clearInterval(pinging);
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

  const _close = server.close;

  server.close = () => {
    localNode.gracefulShutdown();

    return _close.call(server);
  };

  Object.defineProperty(server, "localNode", { value: localNode });

  server.listen(port ? parseInt(port) : undefined, host);

  return new Promise((resolve) => {
    server.once("listening", () => {
      resolve(server as SyncServer);
    });
  });
};
