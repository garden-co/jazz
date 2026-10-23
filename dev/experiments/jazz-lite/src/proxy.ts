/**
 * Jazz Lite client proxy (experimental, Node).
 *
 * One backend Jazz client (NAPI runtime, connected upstream like any other
 * backend) serves every lite connection. Each connection authenticates once
 * with a bearer token and gets a session-scoped `Db` from `forRequest()`, so
 * every query and write is evaluated with that user's permissions, exactly as
 * an HTTP route using `forRequest()` would be. The proxy never widens access.
 *
 * Per subscription the proxy keeps the last result (id → encoded row) and sends
 * only rows that changed, ids that left, and the order when it moved.
 */
import type { Db, QueryBuilder, TableProxy } from "jazz-tools/backend";
import {
  decodeValue,
  encodeValue,
  toWireError,
  type ClientMessage,
  type ServerMessage,
  type WriteTier,
} from "./protocol.js";

/** The subset of a `ws`/WHATWG socket the proxy needs. */
export interface ProxySocket {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  on(event: "message", listener: (data: unknown) => void): void;
  on(event: "close", listener: () => void): void;
}

export interface LiteProxyBackend {
  forRequest(request: { headers: Record<string, string> }): Promise<Db>;
}

export interface LiteProxyOptions {
  backend: LiteProxyBackend;
  /** The same `defineApp` result the browser uses. Tables are resolved by name. */
  app: Record<string, unknown>;
}

export interface LiteProxyStats {
  connections: number;
  subscriptions: number;
  framesOut: number;
  bytesOut: number;
  framesIn: number;
  bytesIn: number;
}

type AnyTable = TableProxy<any, any>;

interface SubscriptionState {
  unsubscribe: () => void;
  last: Map<string, string>;
  order: string[];
  seeded: boolean;
}

export function createLiteProxy(options: LiteProxyOptions) {
  const stats: LiteProxyStats = {
    connections: 0,
    subscriptions: 0,
    framesOut: 0,
    bytesOut: 0,
    framesIn: 0,
    bytesIn: 0,
  };

  function table(name: string): AnyTable {
    const candidate = options.app[name] as AnyTable | undefined;
    if (!candidate || typeof candidate !== "object" || candidate._table !== name) {
      throw new Error(`Unknown table ${JSON.stringify(name)}`);
    }
    return candidate;
  }

  /** Rebuild a query handle from browser query JSON against the server's own schema. */
  function queryFor(name: string, json: string): QueryBuilder<any> {
    const base = table(name) as AnyTable & {
      _columnTransformsByTable?: unknown;
    };
    return {
      _table: base._table,
      _schema: base._schema,
      _columnTransforms: base._columnTransforms,
      _columnTransformsByTable: base._columnTransformsByTable,
      _build: () => json,
    } as unknown as QueryBuilder<any>;
  }

  function readOptions(raw: unknown): Record<string, unknown> | undefined {
    if (!raw || typeof raw !== "object") return undefined;
    const { tier, firstLoadRemoteWaitMs, branch, base } = raw as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    if (tier !== undefined) out.tier = tier;
    if (firstLoadRemoteWaitMs !== undefined) out.firstLoadRemoteWaitMs = firstLoadRemoteWaitMs;
    if (branch !== undefined) out.branch = branch;
    if (base !== undefined) out.base = base;
    return out;
  }

  function handle(socket: ProxySocket): void {
    stats.connections++;
    let db: Db | null = null;
    let authenticating = false;
    let nextTx = 1;
    const subscriptions = new Map<number, SubscriptionState>();
    const transactions = new Map<number, ReturnType<Db["beginTransaction"]>>();

    const send = (message: ServerMessage) => {
      const frame = JSON.stringify(message);
      stats.framesOut++;
      stats.bytesOut += frame.length;
      socket.send(frame);
    };
    const reply = (id: number, work: () => unknown) => {
      Promise.resolve()
        .then(work)
        .then(
          (v) => send(v === undefined ? { t: "ok", id } : { t: "ok", id, v }),
          (error) => send({ t: "err", id, e: toWireError(error) }),
        );
    };
    const tx = (id: number | undefined) => {
      if (id === undefined) return undefined;
      const open = transactions.get(id);
      if (!open) throw new Error(`Unknown or finished transaction ${id}`);
      return open;
    };

    socket.on("message", (data) => {
      const text = String(data);
      stats.framesIn++;
      stats.bytesIn += text.length;
      let message: ClientMessage;
      try {
        message = JSON.parse(text) as ClientMessage;
      } catch {
        socket.close(1003, "invalid frame");
        return;
      }

      if (!db) {
        if (message.t !== "auth" || authenticating) {
          send({ t: "fatal", e: { message: "First frame must be auth" } });
          socket.close(1008, "unauthenticated");
          return;
        }
        authenticating = true;
        options.backend.forRequest({ headers: { authorization: `Bearer ${message.token}` } }).then(
          (scoped) => {
            db = scoped;
            send({ t: "ready", account: scoped.getAuthState().session?.user.account ?? null });
          },
          (error) => {
            send({ t: "fatal", e: toWireError(error) });
            socket.close(1008, "unauthenticated");
          },
        );
        return;
      }
      const scoped: Db = db;

      switch (message.t) {
        case "auth":
          send({ t: "fatal", e: { message: "Token refresh is not supported by Jazz Lite yet" } });
          socket.close(1008, "re-auth unsupported");
          return;

        case "query": {
          const { id, table: name, query, opts } = message;
          reply(id, async () => {
            const reader = tx(message.tx) ?? scoped;
            const rows = await reader.all(queryFor(name, query), readOptions(opts) as never);
            return encodeValue(rows);
          });
          return;
        }

        case "sub": {
          const { id, table: name, query, opts } = message;
          if (subscriptions.has(id)) {
            send({ t: "suberr", id, e: { message: `Subscription ${id} already exists` } });
            return;
          }
          const state: SubscriptionState = {
            unsubscribe: () => {},
            last: new Map(),
            order: [],
            seeded: false,
          };
          subscriptions.set(id, state);
          stats.subscriptions++;
          try {
            state.unsubscribe = scoped.subscribe(
              queryFor(name, query),
              {
                onUpdate: (rows: Array<{ id: string }>) => {
                  if (!subscriptions.has(id)) return;
                  const diff = diffResult(state, rows);
                  if (diff) send({ t: "rows", id, ...diff });
                },
                onError: (error: unknown) => {
                  if (subscriptions.delete(id)) stats.subscriptions--;
                  send({ t: "suberr", id, e: toWireError(error) });
                },
              },
              readOptions(opts) as never,
            );
          } catch (error) {
            subscriptions.delete(id);
            stats.subscriptions--;
            send({ t: "suberr", id, e: toWireError(error) });
          }
          return;
        }

        case "unsub": {
          const state = subscriptions.get(message.id);
          if (!state) return;
          subscriptions.delete(message.id);
          stats.subscriptions--;
          state.unsubscribe();
          return;
        }

        case "write": {
          const { id, op, table: name, rowId, data } = message;
          reply(id, async () => {
            const target = table(name);
            const values = decodeValue(data ?? {}) as never;
            const open = tx(message.tx);
            if (open) {
              switch (op) {
                case "insert":
                  return encodeValue(open.insert(target, values));
                case "update":
                  return void open.update(target, requireId(rowId), values);
                case "upsert":
                  return void open.upsert(target, requireId(rowId), values);
                case "delete":
                  return void open.delete(target, requireId(rowId));
              }
            }
            const tier: WriteTier = message.tier ?? "global";
            switch (op) {
              case "insert":
                return encodeValue(await scoped.insert(target, values).wait({ tier }));
              case "update":
                return void (await scoped.update(target, requireId(rowId), values).wait({ tier }));
              case "upsert":
                return void (await scoped.upsert(target, requireId(rowId), values).wait({ tier }));
              case "delete":
                return void (await scoped.delete(target, requireId(rowId)).wait({ tier }));
            }
            throw new Error(`Unknown write op ${JSON.stringify(op)}`);
          });
          return;
        }

        case "begin": {
          const { id, kind } = message;
          reply(id, () => {
            const open =
              kind === "exclusive" ? scoped.beginExclusiveTransaction() : scoped.beginTransaction();
            const txId = nextTx++;
            transactions.set(txId, open as ReturnType<Db["beginTransaction"]>);
            return txId;
          });
          return;
        }

        case "commit": {
          const { id } = message;
          reply(id, async () => {
            const open = tx(message.tx)!;
            transactions.delete(message.tx);
            await open.commit().wait({ tier: message.tier ?? "global" });
          });
          return;
        }

        case "rollback": {
          const { id } = message;
          reply(id, async () => {
            const open = tx(message.tx)!;
            transactions.delete(message.tx);
            await open.rollback();
          });
          return;
        }
      }
    });

    socket.on("close", () => {
      stats.connections--;
      for (const state of subscriptions.values()) state.unsubscribe();
      stats.subscriptions -= subscriptions.size;
      subscriptions.clear();
      // Open transactions die with their connection; they were never committed.
      for (const open of transactions.values()) void open.rollback().catch(() => {});
      transactions.clear();
    });
  }

  return { handle, stats };
}

function requireId(rowId: string | undefined): string {
  if (typeof rowId !== "string") throw new Error("Write requires rowId");
  return rowId;
}

/** Compute a minimal set/del/order change and update the stored snapshot. */
function diffResult(
  state: SubscriptionState,
  rows: Array<{ id: string }>,
): { set?: unknown[]; del?: string[]; order?: string[] } | null {
  const next = new Map<string, string>();
  const set: unknown[] = [];
  const order: string[] = [];
  for (const row of rows) {
    const encoded = encodeValue(row);
    const json = JSON.stringify(encoded);
    next.set(row.id, json);
    order.push(row.id);
    if (state.last.get(row.id) !== json) set.push(encoded);
  }
  const del = state.order.filter((id) => !next.has(id));

  // The client applies del, then appends newly seen ids. Send the order only
  // when that would not reproduce it.
  const removed = new Set(del);
  const implied = state.order.filter((id) => !removed.has(id));
  for (const row of rows) if (!state.last.has(row.id)) implied.push(row.id);
  const orderChanged = implied.length !== order.length || implied.some((id, i) => id !== order[i]);

  // The first result is always sent, even when empty.
  const first = !state.seeded;
  state.seeded = true;
  state.last = next;
  state.order = order;
  if (!first && set.length === 0 && del.length === 0 && !orderChanged) return null;
  return {
    ...(set.length ? { set } : {}),
    ...(del.length ? { del } : {}),
    ...(orderChanged ? { order } : {}),
  };
}
