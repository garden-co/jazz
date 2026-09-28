import { Cause, Context, Effect, Exit, Layer, Option, Queue, Stream } from "effect";
import type { AccountDbConfig } from "../accounts/context.js";
import type { AuthState } from "../runtime/auth-state.js";
import type {
  DurabilityTier,
  MutationErrorEvent,
  PermissionAdvice,
  WriteHandle,
} from "../runtime/client.js";
import type {
  Db,
  DeleteOptions,
  InsertOptions,
  QueryBuilder,
  QueryOptions,
  RestoreOptions,
  ShutdownOptions,
  TableProxy,
  Transaction,
  TransactionKind,
  UpdateOptions,
} from "../runtime/db.js";
import { limitQueryToOne } from "../runtime/db.js";
import { createDb } from "../runtime/default-create-db.js";
import { JazzError, JazzWriteRejected, toJazzError, toWriteError } from "./errors.js";

/**
 * Durability wait for a write. Without `wait` a write effect completes once
 * the write is applied locally, exactly like the core API. With `wait` it
 * also waits until the write is durable at that tier and fails with
 * {@link JazzWriteRejected} if it is rejected on the way.
 */
export interface WaitOptions {
  readonly wait?: DurabilityTier | undefined;
}

/** Row-level operations shared by {@link JazzDb} and {@link JazzTransaction}. */
interface JazzReads {
  /** Run a query once and return every matching row. */
  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Effect.Effect<T[], JazzError>;
  /** Run a query once and return the first matching row, if any. */
  one<T>(
    query: QueryBuilder<T>,
    options?: QueryOptions,
  ): Effect.Effect<Option.Option<T>, JazzError>;
}

/** A Jazz database as an Effect service. Obtain it with `yield* Jazz`. */
export interface JazzDb extends JazzReads {
  /** The underlying core database, for APIs this binding does not wrap. */
  readonly db: Db;

  /**
   * A live query. Emits the complete current result on subscription and
   * again whenever it changes. Each emission is a full snapshot, so a slow
   * consumer only ever sees the newest one (older snapshots are dropped).
   * The subscription is removed when the stream ends or is interrupted.
   */
  stream<T extends { id: string }>(
    query: QueryBuilder<T>,
    options?: QueryOptions,
  ): Stream.Stream<T[], JazzError>;

  insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: InsertOptions & WaitOptions,
  ): Effect.Effect<T, JazzError | JazzWriteRejected>;
  upsert<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions & WaitOptions,
  ): Effect.Effect<void, JazzError | JazzWriteRejected>;
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions & WaitOptions,
  ): Effect.Effect<void, JazzError | JazzWriteRejected>;
  delete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: DeleteOptions & WaitOptions,
  ): Effect.Effect<void, JazzError | JazzWriteRejected>;
  restore<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Init,
    options?: RestoreOptions & WaitOptions,
  ): Effect.Effect<T, JazzError | JazzWriteRejected>;

  /**
   * Run `body` in a mergeable transaction. The transaction commits when
   * `body` succeeds and rolls back when it fails, dies or is interrupted.
   * The effect completes once the commit is applied locally, or durable at
   * `options.wait` when given. Once `body` has succeeded the commit is kept:
   * interrupting the durability wait interrupts the effect but does not undo
   * the commit.
   */
  transaction<A, E, R>(
    body: (tx: JazzTransaction) => Effect.Effect<A, E, R>,
    options?: WaitOptions,
  ): Effect.Effect<A, E | JazzError | JazzWriteRejected, R>;

  /**
   * Run `body` in an exclusive transaction, validated serializably by the
   * authority. Commits and rolls back like {@link transaction}, then waits for
   * the authority's verdict: a conflicting transaction fails with
   * {@link JazzWriteRejected}, which makes `Effect.retry` a natural fit.
   * Interrupting the verdict wait does not withdraw the committed transaction.
   */
  exclusiveTransaction<A, E, R>(
    body: (tx: JazzTransaction) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | JazzError | JazzWriteRejected, R>;

  canInsert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
  ): Effect.Effect<PermissionAdvice, JazzError>;
  canRead<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
  ): Effect.Effect<PermissionAdvice, JazzError>;
  canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Effect.Effect<PermissionAdvice, JazzError>;
  canDelete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
  ): Effect.Effect<PermissionAdvice, JazzError>;

  /** The current auth state, then every change to it. */
  readonly authState: Stream.Stream<AuthState>;
  /**
   * Rejections of writes that nobody waited for. Waiting writes report their
   * rejection as a {@link JazzWriteRejected} failure instead.
   */
  readonly mutationErrors: Stream.Stream<MutationErrorEvent>;
}

/** Operations available inside {@link JazzDb.transaction}. */
export interface JazzTransaction extends JazzReads {
  /** The underlying core transaction. Do not commit or roll it back yourself. */
  readonly transaction: Transaction;
  insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: InsertOptions,
  ): Effect.Effect<T, JazzError>;
  upsert<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): Effect.Effect<void, JazzError>;
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): Effect.Effect<void, JazzError>;
  delete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: DeleteOptions,
  ): Effect.Effect<void, JazzError>;
  restore<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Init,
    options?: RestoreOptions,
  ): Effect.Effect<T, JazzError>;
}

export interface JazzLayerOptions {
  /** Passed to `Db.shutdown` when the layer's scope closes. */
  readonly shutdown?: ShutdownOptions;
}

/**
 * The Jazz database service. The same service works in browsers, React
 * Native, Node clients and backend request handlers.
 *
 * @example
 * ```ts
 * const program = Effect.gen(function* () {
 *   const jazz = yield* Jazz;
 *   const todo = yield* jazz.insert(app.todos, { title: "Buy milk", done: false });
 *   yield* jazz.update(app.todos, todo.id, { done: true }, { wait: "global" });
 * });
 *
 * program.pipe(Effect.provide(Jazz.layer(config)), Effect.runPromise);
 * ```
 */
export class Jazz extends Context.Service<Jazz, JazzDb>()("jazz-tools/effect/Jazz") {
  /** Wrap an existing core {@link Db}. The caller keeps ownership of it. */
  static readonly fromDb = (db: Db): JazzDb => makeJazzDb(db);

  /** Provide {@link Jazz} from an existing core {@link Db} the caller owns. */
  static readonly layerDb = (db: Db): Layer.Layer<Jazz> => Layer.succeed(Jazz, makeJazzDb(db));

  /**
   * Open a Jazz database for the layer's lifetime and shut it down when the
   * layer's scope closes.
   */
  static readonly layer = (
    config: AccountDbConfig,
    options?: JazzLayerOptions,
  ): Layer.Layer<Jazz, JazzError> =>
    Layer.effect(
      Jazz,
      Effect.map(
        Effect.acquireRelease(
          Effect.tryPromise({ try: () => createDb(config), catch: toJazzError("open") }),
          (db) => Effect.ignore(Effect.tryPromise(() => db.shutdown(options?.shutdown))),
        ),
        makeJazzDb,
      ),
    );
}

const waitFor = <H extends WriteHandle<unknown, unknown>>(
  operation: string,
  handle: H,
  wait: DurabilityTier | undefined,
): Effect.Effect<void, JazzError | JazzWriteRejected> =>
  wait === undefined
    ? Effect.void
    : Effect.tryPromise({
        try: () => handle.wait({ tier: wait }),
        catch: toWriteError(operation),
      });

/** Split our `wait` option from the core write options. */
const splitWait = <O extends WaitOptions>(
  options: O | undefined,
): [Omit<O, "wait"> | undefined, DurabilityTier | undefined] => {
  if (options === undefined) return [undefined, undefined];
  const { wait, ...rest } = options;
  return [rest, wait];
};

const makeReads = (target: {
  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>;
}): JazzReads => ({
  all: (query, options) =>
    Effect.tryPromise({ try: () => target.all(query, options), catch: toJazzError("all") }),
  one: (query, options) =>
    Effect.tryPromise({
      // The same lowering as `Db.one` and `Transaction.one`, which return
      // `null` where this returns `Option.none()`.
      try: async () => (await target.all(limitQueryToOne(query), options))[0],
      catch: toJazzError("one"),
    }).pipe(Effect.map(Option.fromUndefinedOr)),
});

function makeJazzDb(db: Db): JazzDb {
  const write = <T>(
    operation: string,
    wait: DurabilityTier | undefined,
    run: () => { readonly value: T } & WriteHandle<unknown, unknown>,
  ): Effect.Effect<T, JazzError | JazzWriteRejected> =>
    Effect.flatMap(Effect.try({ try: run, catch: toJazzError(operation) }), (handle) =>
      Effect.as(waitFor(operation, handle, wait), handle.value),
    );

  const runTransaction = <A, E, R, K extends TransactionKind>(
    kind: K,
    body: (tx: JazzTransaction) => Effect.Effect<A, E, R>,
    wait: DurabilityTier | undefined,
  ): Effect.Effect<A, E | JazzError | JazzWriteRejected, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const transaction: Transaction = yield* Effect.try({
          try: () =>
            kind === "exclusive" ? db.beginExclusiveTransaction() : db.beginTransaction(),
          catch: toJazzError("transaction"),
        });
        const rollback = Effect.ignore(Effect.tryPromise(() => transaction.rollback()));
        const exit = yield* Effect.exit(restore(body(makeJazzTransaction(transaction))));
        if (Exit.isFailure(exit)) {
          yield* rollback;
          return yield* exit;
        }
        const committed = yield* Effect.try({
          try: () => transaction.commit(),
          catch: toJazzError("commit"),
        }).pipe(Effect.tapError(() => rollback));
        // The commit is local once its transaction id resolves; a deferred
        // commit failure (for example a failed read inside the transaction)
        // surfaces here, and the transaction is rolled back like the core does.
        yield* Effect.tryPromise({
          try: () => committed.txId,
          catch: toJazzError("commit"),
        }).pipe(Effect.tapError(() => rollback));
        if (kind === "exclusive") {
          yield* restore(
            Effect.tryPromise({
              try: () =>
                (committed as WriteHandle<unknown, unknown> & { wait(): Promise<unknown> }).wait(),
              catch: toWriteError("exclusiveTransaction"),
            }),
          );
        } else {
          yield* restore(waitFor("transaction", committed, wait));
        }
        return exit.value;
      }),
    );

  const callbackStream = <A>(register: (emit: (value: A) => void) => () => void) =>
    Stream.callback<A>((queue) =>
      Effect.acquireRelease(
        Effect.sync(() => register((value) => Queue.offerUnsafe(queue, value))),
        (stop) => Effect.sync(stop),
      ),
    );

  return {
    db,
    ...makeReads(db),

    stream: <T extends { id: string }>(query: QueryBuilder<T>, options?: QueryOptions) =>
      Stream.callback<T[], JazzError>(
        (queue) =>
          Effect.acquireRelease(
            Effect.try({
              try: () =>
                db.subscribe(
                  query,
                  {
                    onUpdate: (rows) => {
                      Queue.offerUnsafe(queue, rows);
                    },
                    onError: (cause) => {
                      Queue.failCauseUnsafe(
                        queue,
                        Cause.fail(new JazzError({ operation: "subscribe", cause })),
                      );
                    },
                  },
                  options,
                ),
              catch: toJazzError("subscribe"),
            }),
            (unsubscribe) => Effect.sync(unsubscribe),
          ),
        // Every update is the complete result, so only the newest one matters.
        { bufferSize: 1, strategy: "sliding" },
      ),

    insert: (table, data, options) => {
      const [rest, wait] = splitWait(options);
      return write("insert", wait, () => db.insert(table, data, rest));
    },
    upsert: (table, id, data, options) => {
      const [rest, wait] = splitWait(options);
      return Effect.asVoid(write("upsert", wait, () => db.upsert(table, id, data, rest)));
    },
    update: (table, id, data, options) => {
      const [rest, wait] = splitWait(options);
      return Effect.asVoid(write("update", wait, () => db.update(table, id, data, rest)));
    },
    delete: (table, id, options) => {
      const [rest, wait] = splitWait(options);
      return Effect.asVoid(write("delete", wait, () => db.delete(table, id, rest)));
    },
    restore: (table, id, data, options) => {
      const [rest, wait] = splitWait(options);
      return write("restore", wait, () => db.restore(table, id, data, rest));
    },

    transaction: (body, options) => runTransaction("mergeable", body, options?.wait),
    exclusiveTransaction: (body) => runTransaction("exclusive", body, undefined),

    canInsert: (table, data) =>
      Effect.tryPromise({ try: () => db.canInsert(table, data), catch: toJazzError("canInsert") }),
    canRead: (table, id) =>
      Effect.tryPromise({ try: () => db.canRead(table, id), catch: toJazzError("canRead") }),
    canUpdate: (table, id, data) =>
      Effect.tryPromise({
        try: () => db.canUpdate(table, id, data),
        catch: toJazzError("canUpdate"),
      }),
    canDelete: (table, id) =>
      Effect.tryPromise({ try: () => db.canDelete(table, id), catch: toJazzError("canDelete") }),

    // `onAuthChanged` reports the current state as soon as it is attached.
    authState: callbackStream<AuthState>((emit) => db.onAuthChanged(emit)),
    mutationErrors: callbackStream<MutationErrorEvent>((emit) => db.onMutationError(emit)),
  };
}

function makeJazzTransaction(transaction: Transaction): JazzTransaction {
  const sync = <A>(operation: string, run: () => A): Effect.Effect<A, JazzError> =>
    Effect.try({ try: run, catch: toJazzError(operation) });
  return {
    transaction,
    ...makeReads(transaction),
    insert: (table, data, options) =>
      sync("insert", () => transaction.insert(table, data, options)),
    upsert: (table, id, data, options) =>
      sync("upsert", () => transaction.upsert(table, id, data, options)),
    update: (table, id, data, options) =>
      sync("update", () => transaction.update(table, id, data, options)),
    delete: (table, id, options) => sync("delete", () => transaction.delete(table, id, options)),
    restore: (table, id, data, options) =>
      sync("restore", () => transaction.restore(table, id, data, options)),
  };
}
