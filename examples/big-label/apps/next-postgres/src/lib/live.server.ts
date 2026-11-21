// Live updates: one WebSocket per tab, on live-server.ts. A tab
// subscribes to topics (`org:<id>`; its own `me` topic is implicit).
// Writes NOTIFY what they changed inside their transaction (db.server.ts);
// this LISTENs and tells every subscriber which tables changed, and the
// client refetches them over REST. NOTIFY because the API routes run in the
// Next process and this in its own.
import type { Server } from "node:http";
import pg from "pg";
import { WebSocketServer, type WebSocket } from "ws";
import type { ChangeEvent } from "../../schema";
import { auth } from "./auth";
import { CHANNEL, DATABASE_URL, pool, type Changes } from "./db.server";
import { canSubscribe } from "./permissions.server";

type Client = { account: string; topics: Set<string> };
const clients = new Map<WebSocket, Client>();
const subscribers = new Map<string, Set<WebSocket>>();

function join(ws: WebSocket, topic: string) {
  clients.get(ws)?.topics.add(topic);
  const set = subscribers.get(topic) ?? new Set();
  set.add(ws);
  subscribers.set(topic, set);
}

function leave(ws: WebSocket, topic: string) {
  clients.get(ws)?.topics.delete(topic);
  subscribers.get(topic)?.delete(ws);
  if (subscribers.get(topic)?.size === 0) subscribers.delete(topic);
}

function send(ws: WebSocket, topic: string, tables: string[]) {
  const event: ChangeEvent = { type: "change", topic: topic.startsWith("me:") ? "me" : topic, tables };
  ws.send(JSON.stringify(event));
}

function deliver({ changes, revoke }: Changes) {
  for (const [topic, tables] of changes) for (const ws of subscribers.get(topic) ?? []) send(ws, topic, tables);
  for (const [account, topic] of revoke) {
    for (const ws of [...(subscribers.get(topic) ?? [])]) {
      if (clients.get(ws)?.account !== account) continue;
      send(ws, topic, ["*"]);
      leave(ws, topic);
    }
  }
}

export async function attachLive(server: Server) {
  const listener = new pg.Client({ connectionString: DATABASE_URL });
  await listener.connect();
  listener.on("notification", (message) => deliver(JSON.parse(message.payload!)));
  await listener.query(`LISTEN ${CHANNEL}`);

  const wss = new WebSocketServer({ noServer: true });
  server.on("upgrade", async (req, socket, head) => {
    const session = await auth.api.getSession({ headers: new Headers({ cookie: req.headers.cookie ?? "" }) }).catch(() => null);
    if (!session) {
      socket.end("HTTP/1.1 401 Unauthorized\r\n\r\n");
      return;
    }
    const account = session.user.id;
    wss.handleUpgrade(req, socket, head, (ws) => {
      clients.set(ws, { account, topics: new Set() });
      join(ws, `me:${account}`);
      ws.on("message", async (raw) => {
        const msg = JSON.parse(String(raw)) as { type: "sub" | "unsub"; topics: string[] };
        if (msg.type === "unsub") return msg.topics.forEach((topic) => leave(ws, topic));
        const allowed = await Promise.all(msg.topics.map((t) => canSubscribe(pool, account, t).catch(() => false)));
        const subscribed = msg.topics.filter((_, i) => allowed[i]);
        subscribed.forEach((topic) => join(ws, topic));
        ws.send(JSON.stringify({ type: "subscribed", topics: subscribed, denied: msg.topics.filter((_, i) => !allowed[i]) }));
      });
      ws.on("close", () => {
        for (const topic of [...(clients.get(ws)?.topics ?? [])]) leave(ws, topic);
        clients.delete(ws);
      });
    });
  });
}
