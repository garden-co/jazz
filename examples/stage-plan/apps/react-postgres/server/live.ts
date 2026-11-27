// Live updates: one WebSocket per tab. A tab subscribes to topics
// (`show:<id>`; its own `me` topic is implicit); after a write commits, the
// server tells every subscriber which tables changed, and the client refetches
// them over REST. Delivery is the server's own broadcast: there is one server
// process, so no LISTEN/NOTIFY is needed.
import type { Server } from "node:http";
import { WebSocketServer, type WebSocket } from "ws";
import type { ChangeEvent } from "../shared/types.js";
import { pool } from "./db.js";
import { canSubscribe } from "./permissions.js";
import { accountFromCookieHeader } from "./session.js";

type Client = { account: string; topics: Set<string> };
const clients = new Map<WebSocket, Client>();
const subscribers = new Map<string, Set<WebSocket>>();
const meTopic = (account: string) => `me:${account}`;

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

export function publish(topic: string, tables: string[]) {
  for (const ws of subscribers.get(topic) ?? []) send(ws, topic, tables);
}

/** Drops an account's subscriptions to a topic, telling it to refetch (and find it gone). */
export function revoke(account: string, topic: string) {
  for (const ws of [...(subscribers.get(topic) ?? [])]) {
    if (clients.get(ws)?.account !== account) continue;
    send(ws, topic, ["*"]);
    leave(ws, topic);
  }
}

export function attachLive(server: Server) {
  const wss = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    const account = accountFromCookieHeader(req.headers.cookie);
    if (req.url !== "/api/live" || !account) {
      socket.end("HTTP/1.1 401 Unauthorized\r\n\r\n");
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      clients.set(ws, { account, topics: new Set() });
      join(ws, meTopic(account));
      ws.on("message", async (raw) => {
        const msg = JSON.parse(String(raw)) as { type: "sub" | "unsub"; topic: string };
        if (msg.type === "unsub") return leave(ws, msg.topic);
        const ok = await canSubscribe(pool, account, msg.topic).catch(() => false);
        if (ok) join(ws, msg.topic);
        ws.send(JSON.stringify({ type: ok ? "subscribed" : "denied", topic: msg.topic }));
      });
      ws.on("close", () => {
        for (const topic of [...(clients.get(ws)?.topics ?? [])]) leave(ws, topic);
        clients.delete(ws);
      });
    });
  });
}
