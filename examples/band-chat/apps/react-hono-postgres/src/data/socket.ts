// The live WebSocket: reconnects, keeps refcounted topic subscriptions (sent
// in batches), and hands change events to a listener. The server only lets
// you subscribe to topics you may read; `me` (your own lists) is always on.
import type { ChangeEvent } from "../../shared/types";

type TopicState = "pending" | "subscribed" | "denied";
type Message = ChangeEvent | { type: "subscribed"; topics: string[]; denied: string[] };

const refs = new Map<string, number>();
const states = new Map<string, TopicState>([["me", "pending"]]);
const stateListeners = new Set<() => void>();
let ws: WebSocket | undefined;
let connected = false;
let everConnected = false;
let retryMs = 250;
let queued: { sub: Set<string>; unsub: Set<string> } | undefined;
type Handlers = { onChange(event: ChangeEvent): void; onReconnect(): void; onSubscribed(topics: string[]): void };
let handlers: Handlers | undefined;

const notify = () => stateListeners.forEach((listener) => listener());

/** Sends subscription changes once per tick, so a list of rooms subscribes in one message. */
function queue(kind: "sub" | "unsub", topic: string) {
  if (!queued) {
    queued = { sub: new Set(), unsub: new Set() };
    queueMicrotask(() => {
      const { sub, unsub } = queued!;
      queued = undefined;
      if (!connected) return;
      if (sub.size) ws!.send(JSON.stringify({ type: "sub", topics: [...sub] }));
      if (unsub.size) ws!.send(JSON.stringify({ type: "unsub", topics: [...unsub] }));
    });
  }
  queued[kind === "sub" ? "unsub" : "sub"].delete(topic);
  queued[kind].add(topic);
}

function connect() {
  const socket = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/api/live`);
  ws = socket;
  let opened = false;
  socket.onopen = () => {
    connected = opened = true;
    retryMs = 250;
    states.set("me", "subscribed");
    notify();
    if (refs.size) socket.send(JSON.stringify({ type: "sub", topics: [...refs.keys()] }));
    if (everConnected) handlers?.onReconnect();
    everConnected = true;
  };
  socket.onmessage = (message) => {
    const event = JSON.parse(message.data) as Message;
    if (event.type === "change") {
      if (event.tables.includes("*")) states.set(event.topic, "denied");
      handlers?.onChange(event);
      return;
    }
    const topics = event.topics.filter((topic) => refs.has(topic));
    for (const topic of topics) states.set(topic, "subscribed");
    for (const topic of event.denied) if (refs.has(topic)) states.set(topic, "denied");
    notify();
    if (topics.length) handlers?.onSubscribed(topics);
  };
  socket.onclose = () => {
    connected = false;
    for (const topic of states.keys()) states.set(topic, "pending");
    notify();
    if (opened || retryMs < 10_000) setTimeout(connect, (retryMs = Math.min(retryMs * 2, 10_000)));
  };
}

export function startLive(h: Handlers) {
  handlers = h;
  if (!ws) connect();
}

/** Subscribes to a topic while the returned function hasn't been called. */
export function subscribe(topic: string) {
  if (topic === "me") return () => {};
  const count = refs.get(topic) ?? 0;
  refs.set(topic, count + 1);
  if (count === 0) {
    states.set(topic, "pending");
    queue("sub", topic);
  }
  return () => {
    const left = refs.get(topic)! - 1;
    if (left > 0) return void refs.set(topic, left);
    refs.delete(topic);
    states.delete(topic);
    queue("unsub", topic);
  };
}

/** A topic is settled once the server accepted or refused it. */
export const topicSettled = (topic: string) => (states.get(topic) ?? "pending") !== "pending";
export function onLiveState(listener: () => void) {
  stateListeners.add(listener);
  return () => void stateListeners.delete(listener);
}
