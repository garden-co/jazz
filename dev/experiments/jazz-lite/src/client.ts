/**
 * Jazz Lite browser client (experimental).
 *
 * Same query/transaction surface as `Db`, but no storage, no worker and no
 * WASM: every read and write goes to a server-side client proxy over one
 * WebSocket. Only types are imported from jazz-tools, so this module adds no
 * jazz-tools runtime code to a bundle; the app's own `defineApp` schema (and
 * the query builder behind it) is the only jazz-tools code a lite app ships.
 */
import type { QueryBuilder, QueryOptions, TableProxy } from "jazz-tools";
import {
  decodeValue,
  encodeValue,
  type ClientMessage,
  type ServerMessage,
  type TxKind,
  type WireError,
  type WriteOp,
  type WriteTier,
} from "./protocol.js";

export interface LiteDbConfig {
  /** WebSocket URL of the Jazz Lite proxy, e.g. `wss://example.com/jazz-lite`. */
  url: string;
  /** Bearer token for the user; called on every (re)connect. */
  getToken: () => string | Promise<string>;
  /** Tier writes wait for before their promise resolves. Defaults to "global". */
  defaultWriteTier?: WriteTier;
  /** Override for tests / non-browser hosts. */
  WebSocket?: typeof WebSocket;
}

export interface LiteSubscriptionCallbacks<T> {
  onUpdate: (rows: T[]) => void;
  onError?: (error: Error) => void;
}

export interface LiteWriteOptions {
  tier?: WriteTier;
}

/** Error raised for operations whose outcome the client cannot know. */
export class LiteConnectionLostError extends Error {
  override name = "LiteConnectionLostError";
}

export class LiteRemoteError extends Error {
  readonly code: string | undefined;
  readonly remoteName: string | undefined;
  constructor(wire: WireError) {
    super(wire.message);
    this.name = "LiteRemoteError";
    this.code = wire.code;
    this.remoteName = wire.name;
  }
}

type Pending = { resolve: (v: unknown) => void; reject: (e: Error) => void };

interface ActiveSubscription {
  table: string;
  query: string;
  opts: QueryOptions | undefined;
  callbacks: LiteSubscriptionCallbacks<any>;
  rows: Map<string, unknown>;
  order: string[];
}

type Row = { id: string };
/** Messages that expect an ok/err reply; `id` is assigned by `_request`. */
type RequestMessage = Extract<
  ClientMessage,
  { t: "query" | "write" | "begin" | "commit" | "rollback" }
>;

export class LiteTransaction {
  private done = false;
  constructor(
    private readonly db: LiteDb,
    private readonly tx: Promise<number>,
  ) {}

  private async txId(): Promise<number> {
    if (this.done) throw new Error("Jazz Lite transaction is already finished");
    return this.tx;
  }

  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    return this.db._query(query, options, await this.txId());
  }

  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    return (await this.all(limitOne(query), options))[0] ?? null;
  }

  async insert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<T> {
    return (await this.db._write(
      "insert",
      table._table,
      undefined,
      data,
      undefined,
      await this.txId(),
    )) as T;
  }

  async update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Promise<void> {
    await this.db._write("update", table._table, id, data, undefined, await this.txId());
  }

  async upsert<T, Init>(table: TableProxy<T, Init>, id: string, data: Init): Promise<void> {
    await this.db._write("upsert", table._table, id, data, undefined, await this.txId());
  }

  async delete<T, Init>(table: TableProxy<T, Init>, id: string): Promise<void> {
    await this.db._write("delete", table._table, id, undefined, undefined, await this.txId());
  }

  /** @internal */
  async _finish(commit: boolean, tier: WriteTier | undefined): Promise<void> {
    const tx = await this.txId();
    this.done = true;
    if (commit) await this.db._request({ t: "commit", id: 0, tx, ...(tier ? { tier } : {}) });
    else await this.db._request({ t: "rollback", id: 0, tx });
  }
}

export class LiteDb {
  private socket: WebSocket | null = null;
  private opened: Promise<void> | null = null;
  private nextId = 1;
  private readonly pending = new Map<number, Pending>();
  private readonly subscriptions = new Map<number, ActiveSubscription>();
  private closed = false;
  private retryMs = 250;
  private account: string | null = null;

  constructor(private readonly config: LiteDbConfig) {}

  /** Connect and authenticate now instead of on first use. */
  ready(): Promise<void> {
    return this.ensureOpen();
  }

  /** The authenticated account id, once connected. */
  get accountId(): string | null {
    return this.account;
  }

  // -- reads -----------------------------------------------------------------

  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    return this._query(query, options, undefined);
  }

  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    return (await this.all(limitOne(query), options))[0] ?? null;
  }

  subscribe<T extends Row>(
    query: QueryBuilder<T>,
    callbacks: ((rows: T[]) => void) | LiteSubscriptionCallbacks<T>,
    options?: QueryOptions,
  ): () => void {
    const id = this.nextId++;
    const sub: ActiveSubscription = {
      table: query._table,
      query: query._build(),
      opts: options,
      callbacks: typeof callbacks === "function" ? { onUpdate: callbacks } : callbacks,
      rows: new Map(),
      order: [],
    };
    this.subscriptions.set(id, sub);
    void this.ensureOpen().then(
      () => this.sendSubscribe(id, sub),
      (error) => this.failSubscription(id, error),
    );
    return () => {
      if (!this.subscriptions.delete(id)) return;
      if (this.socket?.readyState === 1) this.send({ t: "unsub", id });
    };
  }

  // -- writes ----------------------------------------------------------------

  async insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: LiteWriteOptions,
  ): Promise<T> {
    return (await this._write("insert", table._table, undefined, data, options?.tier)) as T;
  }

  async update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: LiteWriteOptions,
  ): Promise<void> {
    await this._write("update", table._table, id, data, options?.tier);
  }

  async upsert<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Init,
    options?: LiteWriteOptions,
  ): Promise<void> {
    await this._write("upsert", table._table, id, data, options?.tier);
  }

  async delete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: LiteWriteOptions,
  ): Promise<void> {
    await this._write("delete", table._table, id, undefined, options?.tier);
  }

  /**
   * Interactive transaction held open on the proxy. Reads inside it see its own
   * writes; each read is a round trip. Throwing from the callback rolls back.
   */
  transaction<R>(
    callback: (tx: LiteTransaction) => Promise<R> | R,
    options?: LiteWriteOptions,
  ): Promise<R> {
    return this.runTransaction("mergeable", callback, options);
  }

  exclusiveTransaction<R>(
    callback: (tx: LiteTransaction) => Promise<R> | R,
    options?: LiteWriteOptions,
  ): Promise<R> {
    return this.runTransaction("exclusive", callback, options);
  }

  close(): void {
    this.closed = true;
    this.socket?.close();
    this.rejectAll(new LiteConnectionLostError("Jazz Lite client closed"));
  }

  // -- internals ---------------------------------------------------------------

  private async runTransaction<R>(
    kind: TxKind,
    callback: (tx: LiteTransaction) => Promise<R> | R,
    options: LiteWriteOptions | undefined,
  ): Promise<R> {
    const tx = new LiteTransaction(
      this,
      this._request({ t: "begin", id: 0, kind }) as Promise<number>,
    );
    let result: R;
    try {
      result = await callback(tx);
    } catch (error) {
      await tx._finish(false, undefined).catch(() => {});
      throw error;
    }
    await tx._finish(true, options?.tier ?? this.config.defaultWriteTier);
    return result;
  }

  /** @internal */
  async _query<T>(
    query: QueryBuilder<T>,
    options: QueryOptions | undefined,
    tx: number | undefined,
  ): Promise<T[]> {
    const rows = await this._request({
      t: "query",
      id: 0,
      table: query._table,
      query: query._build(),
      ...(options ? { opts: options } : {}),
      ...(tx === undefined ? {} : { tx }),
    });
    return decodeValue(rows) as T[];
  }

  /** @internal */
  async _write(
    op: WriteOp,
    table: string,
    rowId: string | undefined,
    data: unknown,
    tier: WriteTier | undefined,
    tx?: number,
  ): Promise<unknown> {
    const effectiveTier = tx === undefined ? (tier ?? this.config.defaultWriteTier) : undefined;
    const value = await this._request({
      t: "write",
      id: 0,
      op,
      table,
      ...(rowId === undefined ? {} : { rowId }),
      ...(data === undefined ? {} : { data: encodeValue(data) }),
      ...(effectiveTier ? { tier: effectiveTier } : {}),
      ...(tx === undefined ? {} : { tx }),
    });
    return decodeValue(value);
  }

  /** @internal */
  async _request(message: RequestMessage): Promise<unknown> {
    await this.ensureOpen();
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.send({ ...message, id });
    });
  }

  private send(message: ClientMessage): void {
    this.socket!.send(JSON.stringify(message));
  }

  private ensureOpen(): Promise<void> {
    if (this.closed) return Promise.reject(new LiteConnectionLostError("Jazz Lite client closed"));
    this.opened ??= this.connect().catch((error: unknown) => {
      this.opened = null;
      throw error;
    });
    return this.opened;
  }

  private async connect(): Promise<void> {
    const WS = this.config.WebSocket ?? WebSocket;
    const token = await this.config.getToken();
    const socket = new WS(this.config.url);
    this.socket = socket;
    await new Promise<void>((resolve, reject) => {
      let ready = false;
      socket.onopen = () =>
        socket.send(JSON.stringify({ t: "auth", token } satisfies ClientMessage));
      socket.onmessage = (event) => {
        const message = JSON.parse(String(event.data)) as ServerMessage;
        if (!ready) {
          if (message.t === "ready") {
            ready = true;
            this.account = message.account;
            this.retryMs = 250;
            resolve();
          } else if (message.t === "fatal") {
            reject(new LiteRemoteError(message.e));
          }
          return;
        }
        this.onMessage(message);
      };
      socket.onclose = () => {
        if (!ready) reject(new LiteConnectionLostError("Jazz Lite connection failed"));
        else this.onDisconnect();
      };
    });
  }

  private onMessage(message: ServerMessage): void {
    switch (message.t) {
      case "ok":
      case "err": {
        const pending = this.pending.get(message.id);
        if (!pending) return;
        this.pending.delete(message.id);
        if (message.t === "ok") pending.resolve(message.v);
        else pending.reject(new LiteRemoteError(message.e));
        return;
      }
      case "rows": {
        const sub = this.subscriptions.get(message.id);
        if (!sub) return;
        for (const id of message.del ?? []) sub.rows.delete(id);
        const added: string[] = [];
        for (const raw of message.set ?? []) {
          const row = decodeValue(raw) as Row;
          if (!sub.rows.has(row.id)) added.push(row.id);
          sub.rows.set(row.id, row);
        }
        if (message.order) sub.order = message.order;
        else if (message.del?.length || added.length) {
          const removed = new Set(message.del);
          sub.order = sub.order.filter((id) => !removed.has(id)).concat(added);
        }
        sub.callbacks.onUpdate(sub.order.map((id) => sub.rows.get(id)));
        return;
      }
      case "suberr":
        this.failSubscription(message.id, new LiteRemoteError(message.e));
        return;
      case "fatal":
        this.rejectAll(new LiteRemoteError(message.e));
        return;
    }
  }

  private sendSubscribe(id: number, sub: ActiveSubscription): void {
    if (!this.subscriptions.has(id)) return;
    sub.rows.clear();
    sub.order = [];
    this.send({
      t: "sub",
      id,
      table: sub.table,
      query: sub.query,
      ...(sub.opts ? { opts: sub.opts } : {}),
    });
  }

  private failSubscription(id: number, error: Error): void {
    const sub = this.subscriptions.get(id);
    if (!sub) return;
    this.subscriptions.delete(id);
    if (sub.callbacks.onError) sub.callbacks.onError(error);
    else console.error("Jazz Lite subscription failed", error);
  }

  /**
   * Requests in flight have unknown outcomes once the socket drops: they are
   * rejected, never retried. Subscriptions are re-established with a full
   * result after reconnecting.
   */
  private onDisconnect(): void {
    this.socket = null;
    this.opened = null;
    this.account = null;
    for (const [, pending] of this.pending) {
      pending.reject(new LiteConnectionLostError("Jazz Lite connection lost; outcome unknown"));
    }
    this.pending.clear();
    if (this.closed) return;
    const delay = this.retryMs;
    this.retryMs = Math.min(this.retryMs * 2, 10_000);
    setTimeout(() => {
      if (this.closed || this.subscriptions.size === 0) return;
      void this.ensureOpen().then(
        () => {
          for (const [id, sub] of this.subscriptions) this.sendSubscribe(id, sub);
        },
        () => this.onDisconnect(),
      );
    }, delay);
  }

  private rejectAll(error: Error): void {
    for (const [, pending] of this.pending) pending.reject(error);
    this.pending.clear();
    for (const id of this.subscriptions.keys()) this.failSubscription(id, error);
  }
}

function limitOne<T>(query: QueryBuilder<T>): QueryBuilder<T> {
  const limited = (query as unknown as { limit?: (n: number) => QueryBuilder<T> }).limit;
  return typeof limited === "function" ? limited.call(query, 1) : query;
}

export function createLiteDb(config: LiteDbConfig): LiteDb {
  return new LiteDb(config);
}
