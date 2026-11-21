import {
  createRealtimeClient,
  QueryRefreshController,
  type LiveQuerySnapshot,
  type MaterializedLiveQuerySubscription,
  type SealedLiveQuery,
} from "@neon/realtime/client";
import { useEffect, useMemo, useSyncExternalStore } from "react";
import type { QueryDef } from "../../queries";

/**
 * Neon Realtime on the client: one shared client, live queries sealed by the
 * app's server, and optimistic writes that drop once their transaction has
 * synced.
 *
 * `@neon/realtime-react`'s `useLiveQuery` takes a sealed query that the caller
 * already has; the BandChat components call their queries inline (as they do
 * on Jazz), so `useQuery` fetches the sealed query and subscribes the way that
 * binding does (a materialized subscription + `QueryRefreshController`).
 */
export const client = createRealtimeClient({
  url: import.meta.env.VITE_NEON_REALTIME_URL,
  logLevel: "warn",
});

/** The signed-in user's Managed Better Auth JWT, sent as a bearer token. */
let authHeaders: () => Promise<Record<string, string>> = async () => ({});
export function setAuthHeaders(headers: typeof authHeaders) {
  authHeaders = headers;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json", ...(await authHeaders()) },
    credentials: "include",
    body: JSON.stringify(body),
  });
  const json = await response.json();
  if (!response.ok) throw new Error(json.error ?? `${path}: ${response.status}`);
  return json as T;
}

const seal = (def: QueryDef<unknown, unknown>) =>
  post<SealedLiveQuery<unknown>>("/api/live", { name: def.name, args: def.args });

type Status = LiveQuerySnapshot<unknown>["status"];
type Rows = readonly Record<string, unknown>[];

/** One live query: sealed on first use, subscribed while a component reads it. */
class LiveStore {
  private listeners = new Set<() => void>();
  private subscription?: MaterializedLiveQuerySubscription<unknown>;
  private refresh?: QueryRefreshController<unknown>;
  private closed = false;
  snapshot: { status: Status; data?: Rows } = { status: "connecting" };

  constructor(readonly def: QueryDef<unknown, unknown>) {}

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    if (this.listeners.size === 1) this.open();
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) this.close();
    };
  };
  getSnapshot = () => this.snapshot;

  private open() {
    this.closed = false;
    stores.add(this);
    void seal(this.def).then((query) => {
      if (this.closed) return;
      const subscription = client.subscribe(query);
      this.subscription = subscription;
      this.refresh = new QueryRefreshController({
        query,
        refreshQuery: () => seal(this.def),
        renewSubscription: (next) => subscription.renew(next),
        onRefreshExhausted: (error) => console.error("Live query renewal stopped", error),
        subscription,
      });
      this.refresh.setSubscription(subscription);
      this.refresh.start();
      const update = (snapshot: LiveQuerySnapshot<unknown>) => {
        this.snapshot = { status: snapshot.status, data: snapshot.data as Rows | undefined };
        this.notify();
      };
      subscription.onChange(update);
      update(subscription.getSnapshot());
    }, (error) => console.error(`Could not open ${this.def.name}`, error));
  }

  private close() {
    this.closed = true;
    stores.delete(this);
    this.refresh?.stop();
    this.subscription?.unsubscribe();
    this.subscription = undefined;
    this.snapshot = { status: "connecting" };
    notifyConnection();
  }

  private notify() {
    for (const listener of this.listeners) listener();
    notifyConnection();
  }

  /** A pending write that touches this query started or ended: render again. */
  overlayChanged() {
    this.snapshot = { ...this.snapshot };
    for (const listener of this.listeners) listener();
  }

  awaitTxId(txid: string, timeout: number) {
    return this.subscription?.awaitTxId(txid, timeout) ?? Promise.resolve();
  }
}

const stores = new Set<LiveStore>();

// ---- Optimistic writes (Neon's mutations guide, "without a framework") -----

/** How a pending write changes the rows of the queries it touches, by query name. */
export type Overlay = Record<string, (rows: Rows, args: Record<string, unknown>) => Rows>;
/** A named write, its arguments, and how it changes the live queries at once. */
export type MutationDef = { name: string; args: unknown; overlay?: (userID: string) => Overlay };
type Pending = { overlay: Overlay; touched: Set<LiveStore> };

const pending = new Map<number, Pending>();
let nextPending = 0;
/** Re-renders only the live queries the write's overlay touches. */
const changedOverlay = (overlay: Overlay) => {
  for (const store of stores) if (overlay[store.def.name]) store.overlayChanged();
};

function withOverlay(store: LiveStore, rows: Rows): Rows {
  let result = rows;
  for (const write of pending.values()) {
    const apply = write.overlay[store.def.name];
    if (!apply) continue;
    const next = apply(result, store.def.args);
    if (next !== result) write.touched.add(store);
    result = next;
  }
  return result;
}

export type MutationResponse = { kind: "committed"; txid: string; result?: unknown } | { kind: "unchanged"; result?: unknown };
export type WriteResult = { type: "success"; result?: unknown } | { type: "error"; error: { message: string } };
export type Write = { client: Promise<void>; server: Promise<WriteResult> };

type ErrorListener = (message: string) => void;
const errorListeners = new Set<ErrorListener>();
export function onWriteError(listener: ErrorListener) {
  errorListeners.add(listener);
  return () => void errorListeners.delete(listener);
}

/**
 * Applies `overlay` at once, sends the write to the app's server, waits until
 * the live queries it touched have synced the write's transaction, then drops
 * the overlay. A rejected write drops it straight away.
 */
export function mutate(name: string, args: unknown, overlay: Overlay = {}): Write {
  const key = nextPending++;
  const write: Pending = { overlay, touched: new Set() };
  pending.set(key, write);
  changedOverlay(overlay);
  const server = (async (): Promise<WriteResult> => {
    try {
      const response = await post<MutationResponse>(`/api/mutate/${name}`, args);
      if (response.kind === "committed") {
        await Promise.allSettled([...write.touched].map((store) => store.awaitTxId(response.txid, 10_000)));
      }
      return { type: "success", result: response.result };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      for (const listener of errorListeners) listener(message);
      return { type: "error", error: { message } };
    } finally {
      pending.delete(key);
      changedOverlay(overlay);
    }
  })();
  return { client: Promise.resolve(), server };
}

// ---- React -----------------------------------------------------------------

const storeCache = new Map<string, { store: LiveStore; users: number }>();

const IDLE_SNAPSHOT: LiveStore["snapshot"] = { status: "connecting" };
const idleStore = { subscribe: () => () => {}, getSnapshot: () => IDLE_SNAPSHOT };

function useStore(def: QueryDef<unknown, unknown> | undefined) {
  const key = def ? `${def.name} ${JSON.stringify(def.args)}` : "";
  const store = useMemo(() => (def ? (storeCache.get(key)?.store ?? new LiveStore(def)) : undefined), [key]);
  useEffect(() => {
    if (!store) return;
    const entry = storeCache.get(key) ?? { store, users: 0 };
    entry.users++;
    storeCache.set(key, entry);
    return () => {
      if (--entry.users === 0) storeCache.delete(key);
    };
  }, [key, store]);
  return store;
}

/**
 * Subscribes to a live query; `type` is "unknown" until its first result. An
 * undefined query subscribes to nothing (as Zero's `useQuery` allows).
 */
export function useQuery<Row, Result>(def: QueryDef<Row, Result>): [Result, { type: "unknown" | "complete" }];
export function useQuery<Row, Result>(def: QueryDef<Row, Result> | undefined): [Result | undefined, { type: "unknown" | "complete" }];
export function useQuery<Row, Result>(def: QueryDef<Row, Result> | undefined) {
  const store = useStore(def as QueryDef<unknown, unknown> | undefined);
  const source = store ?? idleStore;
  const snapshot = useSyncExternalStore(source.subscribe, source.getSnapshot);
  const rows = useMemo(() => (store ? withOverlay(store, snapshot.data ?? []) : undefined), [store, snapshot]);
  const type = snapshot.data === undefined ? "unknown" : "complete";
  if (!def) return [undefined, { type }];
  return [(def.one ? rows?.[0] : rows) as Result, { type }];
}

// ---- Connection state for the top nav ---------------------------------------

const connectionListeners = new Set<() => void>();
let connected = true;
function notifyConnection() {
  const next = [...stores].every((store) => store.snapshot.status !== "stale" && store.snapshot.status !== "error");
  if (next === connected) return;
  connected = next;
  for (const listener of connectionListeners) listener();
}

/** "connected" unless a live query has gone stale or failed. */
export function useConnectionState() {
  const isConnected = useSyncExternalStore(
    (listener) => {
      connectionListeners.add(listener);
      return () => void connectionListeners.delete(listener);
    },
    () => connected,
  );
  return { name: isConnected ? "connected" : "disconnected" };
}

/** The signed-in user's writes, the way the components call them (`db.mutate(mutators.x.y(args))`). */
export function writer(userID: string) {
  return { userID, mutate: (def: MutationDef) => mutate(def.name, def.args, def.overlay?.(userID)) };
}
export type Db = ReturnType<typeof writer>;
