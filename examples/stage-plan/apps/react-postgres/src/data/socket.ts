// The live WebSocket: reconnects, keeps refcounted topic subscriptions, and
// hands change events to a listener. The server only lets you subscribe to
// topics you may read; `me` (your own rows) is always on.
import type { ChangeEvent } from "../../shared/types.js";

type TopicState = "pending" | "subscribed" | "denied";
type Message = ChangeEvent | { type: "subscribed" | "denied"; topic: string };

const refs = new Map<string, number>();
const states = new Map<string, TopicState>([["me", "pending"]]);
const stateListeners = new Set<() => void>();
let ws: WebSocket | undefined;
let connected = false;
let retryMs = 250;
let onChange: (event: ChangeEvent) => void = () => {};
let onReconnect: () => void = () => {};
let onSubscribed: (topic: string) => void = () => {};
let everConnected = false;

const notify = () => stateListeners.forEach((listener) => listener());

function setState(topic: string, state: TopicState) {
  states.set(topic, state);
  notify();
}

function connect() {
  const url = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/api/live`;
  const socket = new WebSocket(url);
  ws = socket;
  let wasConnected = false;
  socket.onopen = () => {
    connected = wasConnected = true;
    retryMs = 250;
    setState("me", "subscribed");
    for (const topic of refs.keys()) socket.send(JSON.stringify({ type: "sub", topic }));
    if (everConnected) onReconnect();
    everConnected = true;
  };
  socket.onmessage = (message) => {
    const event = JSON.parse(message.data) as Message;
    if (event.type === "change") onChange(event);
    else if (refs.has(event.topic)) {
      setState(event.topic, event.type);
      if (event.type === "subscribed") onSubscribed(event.topic);
    }
    if (event.type === "change" && event.tables.includes("*")) setState(event.topic, "denied");
  };
  socket.onclose = () => {
    connected = false;
    for (const topic of states.keys()) states.set(topic, "pending");
    notify();
    if (wasConnected || retryMs < 10_000) setTimeout(connect, (retryMs = Math.min(retryMs * 2, 10_000)));
  };
}

export function startLive(handlers: {
  onChange: typeof onChange;
  onReconnect: typeof onReconnect;
  onSubscribed: typeof onSubscribed;
}) {
  ({ onChange, onReconnect, onSubscribed } = handlers);
  if (!ws) connect();
}

/** Subscribes to a topic while the returned function hasn't been called. */
export function subscribe(topic: string) {
  if (topic === "me") return () => {};
  const count = refs.get(topic) ?? 0;
  refs.set(topic, count + 1);
  if (count === 0) {
    setState(topic, "pending");
    if (connected) ws!.send(JSON.stringify({ type: "sub", topic }));
  }
  return () => {
    const left = refs.get(topic)! - 1;
    if (left > 0) return void refs.set(topic, left);
    refs.delete(topic);
    states.delete(topic);
    if (connected) ws!.send(JSON.stringify({ type: "unsub", topic }));
  };
}

/** A topic is settled once the server accepted or refused it. */
export const topicSettled = (topic: string) => (states.get(topic) ?? "pending") !== "pending";
export const isConnected = () => connected;
export function onLiveState(listener: () => void) {
  stateListeners.add(listener);
  return () => void stateListeners.delete(listener);
}
