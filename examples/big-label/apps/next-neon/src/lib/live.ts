"use client";

import {
  createRealtimeClient,
  QueryRefreshController,
  type LiveQuerySnapshot,
  type MaterializedLiveQuerySubscription,
  type SealedLiveQuery,
} from "@neon/realtime/client";
import { useEffect, useMemo, useSyncExternalStore } from "react";
import type { QueryDef } from "../queries";
import { write as writeAction } from "../../app/actions";

/**
 * Neon Realtime on the client: one shared client, live queries sealed by the
 * app's server, and optimistic writes that drop once their transaction has
 * synced.
 *
 * `@neon/realtime-react`'s `useLiveQuery` takes a sealed query that the caller
 * already has; the BigLabel components call their queries inline (as they do
 * on Jazz), so `useAll`/`useOne` fetch the sealed query and subscribe the way
 * that binding does (a materialized subscription + `QueryRefreshController`).
 */
// Creating the client opens no socket, so this is safe during the server
// render; the socket opens on the first subscribe.
export const client = createRealtimeClient({
  url: process.env.NEXT_PUBLIC_NEON_REALTIME_URL!,
  logLevel: "warn",
});

async function post<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
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
  }

  private notify() {
    for (const listener of this.listeners) listener();
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

export type WriteResult = { type: "success"; result?: unknown } | { type: "error"; error: { message: string } };
/**
 * `committed`: the server's answer (the transaction committed, or was refused);
 * `server`: that, and the live queries the write touched have synced it.
 */
export type Write = { committed: Promise<WriteResult>; server: Promise<WriteResult> };

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
  const fail = (error: unknown): WriteResult => {
    const message = error instanceof Error ? error.message : String(error);
    return { type: "error", error: { message } };
  };
  // Writes are a Server Action (Neon's Next.js example), which Next runs one at a time per page.
  const response = writeAction(name, args);
  const committed = response.then(
    (r): WriteResult => (r.kind === "rejected" ? { type: "error", error: { message: r.error } } : { type: "success" }),
    fail,
  );
  const server = (async (): Promise<WriteResult> => {
    try {
      const r = await response;
      if (r.kind === "rejected") return { type: "error", error: { message: r.error } };
      if (r.kind === "committed") {
        await Promise.allSettled([...write.touched].map((store) => store.awaitTxId(r.txid, 10_000)));
      }
      return { type: "success" };
    } catch (error) {
      return fail(error);
    } finally {
      pending.delete(key);
      changedOverlay(overlay);
    }
  })();
  return { committed, server };
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

function useRows<Row, Result>(def: QueryDef<Row, Result> | undefined) {
  const store = useStore(def as QueryDef<unknown, unknown> | undefined);
  const source = store ?? idleStore;
  const snapshot = useSyncExternalStore(source.subscribe, source.getSnapshot, source.getSnapshot);
  const rows = useMemo(() => (store ? withOverlay(store, snapshot.data ?? []) : undefined), [store, snapshot]);
  return { rows: snapshot.data === undefined ? undefined : (rows as Row[] | undefined), isLoading: snapshot.data === undefined };
}

/** A live query's rows, as Jazz's `useAll`: `data` is undefined until the first result. */
export function useAll<Row>(def: QueryDef<Row, Row[]> | undefined) {
  const { rows, isLoading } = useRows(def);
  return { data: rows, isLoading };
}

/** A live query's first row, as Jazz's `useOne`. */
export function useOne<Row>(def: QueryDef<Row, Row | undefined> | undefined) {
  const { rows, isLoading } = useRows(def);
  return { data: rows?.[0], isLoading };
}

