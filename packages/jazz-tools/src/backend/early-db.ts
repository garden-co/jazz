import {
  WriteHandle,
  WriteResult,
  type JazzClient,
  type WriteWaitOptions,
} from "../runtime/client.js";
import {
  Db,
  previewWrittenRow,
  reserveRowId,
  type DbSubscriptionCallbacks,
  type TableProxy,
} from "../runtime/db.js";

/** Writes that return at once and apply once the store is open. */
const WRITES = new Set(["insert", "restore", "update", "upsert", "delete"]);

/** Methods that already answer later: they wait for the store to open. */
const WAITS = new Set([
  "all",
  "one",
  "canInsert",
  "canRead",
  "canUpdate",
  "canDelete",
  "transaction",
  "serializableTransaction",
  "insertStreaming",
  "updateStreaming",
  "upsertStreaming",
  "captureSnapshot",
  "waitForSync",
  "refreshAccountAuth",
  "openInspectorControlPort",
  "disconnect",
  "reconnect",
  "upgradeToLocalFirst",
  "deleteClientStorage",
  "logout",
  "shutdown",
]);

/** @internal How the early db treats each `Db` method before the store opens (for tests). */
export function earlyDbMethodKind(name: string): "write" | "waits" | "subscribe" | "needs-open" {
  if (WRITES.has(name)) return "write";
  if (WAITS.has(name)) return "waits";
  if (name === "subscribe") return "subscribe";
  return "needs-open";
}

type AnyHandle = WriteHandle<unknown, unknown>;

/** A write made before the store opened, waiting on the write it becomes. */
class EarlyWriteResult<T> extends WriteResult<T> {
  readonly #applied: Promise<AnyHandle>;

  constructor(value: T, applied: Promise<AnyHandle>) {
    super(
      value,
      applied.then((handle) => handle.txId),
      undefined as unknown as JazzClient,
    );
    this.#applied = applied;
  }

  override async wait(options: WriteWaitOptions): Promise<T> {
    await (await this.#applied).wait(options);
    return this.value;
  }

  override mapValue<U>(transformValue: (value: T) => U): WriteResult<U> {
    return new EarlyWriteResult(transformValue(this.value), this.#applied);
  }
}

class EarlyWriteHandle extends WriteHandle {
  readonly #applied: Promise<AnyHandle>;

  constructor(applied: Promise<AnyHandle>) {
    super(
      applied.then((handle) => handle.txId),
      undefined as unknown as JazzClient,
    );
    this.#applied = applied;
  }

  override async wait(options: WriteWaitOptions): Promise<void> {
    await (await this.#applied).wait(options);
  }
}

interface QueuedWrite {
  apply(db: Db): void;
  fail(error: unknown): void;
}

type Method = (...args: unknown[]) => unknown;

/**
 * The backend's own database before its store has opened, and after: once
 * open, every call goes to the `Db` that `live` names then (a reload moves
 * it). Before that, writes return at once (an insert's row, id included)
 * and apply, in order, as the store opens; reads and other async calls
 * wait for it; a subscription starts then. Calls that must answer now,
 * like `getSyncStatus()`, throw until it has opened.
 */
export function earlyDb(open: () => Promise<Db>, live: () => Db | undefined): Db {
  let queue: QueuedWrite[] = [];
  let flushing: Promise<Db> | undefined;

  /** The open db, with the writes queued so far applied to it first. */
  function opened(): Promise<Db> {
    flushing ??= open().then(
      (db) => {
        flushing = undefined;
        const batch = queue;
        queue = [];
        for (const write of batch) write.apply(db);
        return db;
      },
      (error: unknown) => {
        flushing = undefined;
        const batch = queue;
        queue = [];
        for (const write of batch) write.fail(error);
        throw error;
      },
    );
    return flushing;
  }

  // Queued writes go first, so a write made now never overtakes them.
  const now = () => (queue.length === 0 ? live() : undefined);

  function enqueue(write: (db: Db) => AnyHandle): Promise<AnyHandle> {
    const applied = new Promise<AnyHandle>((resolve, reject) => {
      queue.push({
        apply(db) {
          try {
            resolve(write(db));
          } catch (error) {
            reject(error);
          }
        },
        fail: reject,
      });
    });
    // A caller that never waits must not see an unhandled rejection.
    applied.catch(() => {});
    opened().catch(() => {});
    return applied;
  }

  function call(db: Db, name: string, args: unknown[]): unknown {
    return (Reflect.get(db, name, db) as Method).apply(db, args);
  }

  function early(name: string): unknown {
    if (name === "insert" || name === "restore") {
      return (table: TableProxy<unknown, unknown>, ...rest: unknown[]) => {
        const insert = name === "insert";
        const options = (insert ? rest[1] : rest[2]) as { id?: string } | undefined;
        const id = insert ? (options?.id ?? reserveRowId()) : (rest[0] as string);
        const data = insert ? rest[0] : rest[1];
        // The row it writes, from the schema alone, as `Db` gives it.
        const value = previewWrittenRow(table, insert ? "Insert" : "Restore", id, data);
        const args = insert ? [table, data, { ...options, id }] : [table, ...rest];
        return new EarlyWriteResult(
          value,
          enqueue((db) => call(db, name, args) as AnyHandle),
        );
      };
    }
    if (WRITES.has(name)) {
      return (...args: unknown[]) =>
        new EarlyWriteHandle(enqueue((db) => call(db, name, args) as AnyHandle));
    }
    if (WAITS.has(name)) {
      return async (...args: unknown[]) => call(await opened(), name, args);
    }
    if (name === "subscribe") {
      return (...args: unknown[]) => {
        let stop: (() => void) | undefined;
        let stopped = false;
        opened().then(
          (db) => {
            if (!stopped) stop = call(db, "subscribe", args) as () => void;
          },
          (error: unknown) => {
            const callbacks = args[1] as DbSubscriptionCallbacks<{ id: string }> | Method;
            const onError = typeof callbacks === "function" ? undefined : callbacks.onError;
            if (onError) onError(error as Error);
            else console.error("jazz: a subscription could not start", error);
          },
        );
        return () => {
          stopped = true;
          stop?.();
        };
      };
    }
    throw new Error(
      `jazz.db.${name} is not available until the store has opened: \`await jazz.ready()\` first`,
    );
  }

  return new Proxy(Object.create(Db.prototype) as Db, {
    get(_target, name) {
      // Not a thenable, and no well-known symbols: `await jazz.db` stays the db.
      if (typeof name === "symbol" || name === "then") return undefined;
      const db = now();
      if (db) {
        const value = Reflect.get(db, name, db) as unknown;
        return typeof value === "function" ? (value as Method).bind(db) : value;
      }
      return early(name);
    },
  });
}
