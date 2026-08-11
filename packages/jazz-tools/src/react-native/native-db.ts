import type {
  NativeDb,
  NativeDbConstructor,
  PreparedQuery,
  Subscription,
  Transport,
  Tx,
  Write,
} from "../runtime/native-runtime/native-runtime-adapter.js";

type GeneratedPreparedQuery = object;
type GeneratedQueryAttachment = object;

interface GeneratedWriteStateWaiter {
  wait(options?: { signal: AbortSignal }): Promise<void>;
}

interface GeneratedWrite {
  close(): boolean;
  payload(): ArrayBuffer;
  registerWriteStateWaiter(): GeneratedWriteStateWaiter;
  wait(tier: string): void;
  writeState(): string;
}

interface GeneratedSubscriptionEvent {
  eventType: string;
  reset: boolean | undefined;
  delta: ArrayBuffer | undefined;
  relationDelta: ArrayBuffer | undefined;
  settled: boolean | undefined;
  tier: string | undefined;
  reasonJson: string | undefined;
}

interface GeneratedSubscription {
  close(): boolean;
  drain(): GeneratedSubscriptionEvent[];
  readAll(): GeneratedSubscriptionEvent[];
}

interface GeneratedTransport {
  close(): boolean;
  recvWireFrames(): ArrayBuffer[];
  sendWireFrame(frame: ArrayBuffer): void;
  sendWireFrames(frames: ArrayBuffer[]): void;
  tick(): number;
}

interface GeneratedTx {
  commit(): GeneratedWrite;
  delete_(table: string, rowId: ArrayBuffer, updatedAtMs: number | undefined): void;
  insertWithIdEncoded(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): void;
  restoreEncoded(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): void;
  rollback(): void;
  updateEncoded(
    table: string,
    rowId: ArrayBuffer,
    patch: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): void;
  upsertEncoded(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): void;
}

interface GeneratedDb {
  all(query: GeneratedPreparedQuery, optsJson: string | undefined): ArrayBuffer;
  allForIdentity(
    query: GeneratedPreparedQuery,
    author: ArrayBuffer,
    optsJson: string | undefined,
  ): ArrayBuffer;
  allInTransaction(
    query: GeneratedPreparedQuery,
    transaction: GeneratedTx,
    optsJson: string | undefined,
  ): ArrayBuffer;
  allInTransactionForIdentity(
    query: GeneratedPreparedQuery,
    transaction: GeneratedTx,
    author: ArrayBuffer,
    optsJson: string | undefined,
  ): ArrayBuffer;
  allRelationQuery(queryJson: string, optsJson: string | undefined): ArrayBuffer;
  allRelationQueryForIdentity(
    queryJson: string,
    author: ArrayBuffer,
    optsJson: string | undefined,
  ): ArrayBuffer;
  allRelationSnapshot(query: GeneratedPreparedQuery, optsJson: string | undefined): ArrayBuffer;
  allRelationSnapshotForIdentity(
    query: GeneratedPreparedQuery,
    author: ArrayBuffer,
    optsJson: string | undefined,
  ): ArrayBuffer;
  attachQuery(
    query: GeneratedPreparedQuery,
    optsJson: string | undefined,
  ): GeneratedQueryAttachment;
  attachQueryForIdentity(
    query: GeneratedPreparedQuery,
    author: ArrayBuffer,
    optsJson: string | undefined,
  ): GeneratedQueryAttachment;
  canDeleteForIdentity(table: string, rowId: ArrayBuffer, author: ArrayBuffer): boolean;
  canInsertEncoded(table: string, cells: ArrayBuffer): boolean;
  canInsertEncodedForIdentity(table: string, cells: ArrayBuffer, author: ArrayBuffer): boolean;
  canReadForIdentity(table: string, rowId: ArrayBuffer, author: ArrayBuffer): boolean;
  canUpdateEncodedForIdentity(
    table: string,
    rowId: ArrayBuffer,
    patch: ArrayBuffer,
    author: ArrayBuffer,
  ): boolean;
  close(): void;
  connectUpstream(): GeneratedTransport;
  delete_(table: string, rowId: ArrayBuffer, updatedAtMs: number | undefined): GeneratedWrite;
  deleteForIdentity(
    table: string,
    rowId: ArrayBuffer,
    author: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
  detachQuery(attachment: GeneratedQueryAttachment): void;
  exclusiveTx(): GeneratedTx;
  insertWithIdEncoded(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
  insertWithIdEncodedForIdentity(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    author: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
  localCurrentRow(table: string, rowId: ArrayBuffer): ArrayBuffer;
  mergeableTx(): GeneratedTx;
  mergeableTxForIdentity(author: ArrayBuffer): GeneratedTx;
  prepareQuery(query: ArrayBuffer): GeneratedPreparedQuery;
  queryAttachmentIsCovered(attachment: GeneratedQueryAttachment): boolean;
  restoreEncoded(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
  restoreEncodedForIdentity(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    author: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
  setIdentityClaims(author: ArrayBuffer, claimsJson: string | undefined): void;
  setTickScheduler(callback: { onTickNeeded(urgency: string): void }): void;
  subscribe(query: GeneratedPreparedQuery, optsJson: string | undefined): GeneratedSubscription;
  subscribeForIdentity(
    query: GeneratedPreparedQuery,
    author: ArrayBuffer,
    optsJson: string | undefined,
  ): GeneratedSubscription;
  subscribeRelationQuery(queryJson: string, optsJson: string | undefined): GeneratedSubscription;
  subscribeRelationQueryForIdentity(
    queryJson: string,
    author: ArrayBuffer,
    optsJson: string | undefined,
  ): GeneratedSubscription;
  tick(): void;
  updateEncoded(
    table: string,
    rowId: ArrayBuffer,
    patch: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
  updateEncodedForIdentity(
    table: string,
    rowId: ArrayBuffer,
    patch: ArrayBuffer,
    author: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
  upsertEncoded(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
  upsertEncodedForIdentity(
    table: string,
    rowId: ArrayBuffer,
    cells: ArrayBuffer,
    author: ArrayBuffer,
    updatedAtMs: number | undefined,
  ): GeneratedWrite;
}

export interface JazzRnModule {
  RnDb: {
    openMemory(schema: ArrayBuffer, config: ArrayBuffer): GeneratedDb;
    openPersistent(dataPath: string, schema: ArrayBuffer, config: ArrayBuffer): GeneratedDb;
  };
  mintAnonymousToken(
    secret: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;
  mintLocalFirstToken(
    secret: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy.buffer;
}

function toUint8Array(bytes: ArrayBuffer): Uint8Array {
  return new Uint8Array(bytes);
}

function optionalNumber(value: number | null | undefined): number | undefined {
  return value ?? undefined;
}

function jsonArgument(value: unknown): string | undefined {
  return value == null ? undefined : JSON.stringify(value);
}

function parseJson(value: string): unknown {
  return JSON.parse(value) as unknown;
}

function subscriptionEvent(event: GeneratedSubscriptionEvent): unknown {
  switch (event.eventType) {
    case "delta": {
      if (event.delta === undefined || event.relationDelta === undefined) {
        throw new Error("React Native subscription delta is missing binary payloads");
      }
      return {
        type: "delta",
        reset: event.reset === true,
        delta: toUint8Array(event.delta),
        relation_delta: toUint8Array(event.relationDelta),
        settled: event.settled === true,
        tier: event.tier,
      };
    }
    case "rejected": {
      if (event.reasonJson === undefined) {
        throw new Error("React Native subscription rejection is missing its reason");
      }
      return { type: "rejected", reason: parseJson(event.reasonJson) };
    }
    case "closed":
      return { type: "closed" };
    default:
      throw new Error(`Unknown React Native subscription event ${JSON.stringify(event.eventType)}`);
  }
}

function normalizeJazzRnError(error: unknown): unknown {
  if (!(error instanceof Error)) {
    return error;
  }

  const inner = (error as Error & { inner?: { message?: unknown } }).inner;
  if (typeof inner?.message !== "string" || !inner.message.trim()) {
    return error;
  }

  const normalized = new Error(inner.message, { cause: error }) as Error & {
    tag?: unknown;
  };
  normalized.name = error.name;
  normalized.tag = (error as Error & { tag?: unknown }).tag;
  return normalized;
}

function callGenerated<Result>(call: () => Result): Result {
  try {
    return call();
  } catch (error) {
    throw normalizeJazzRnError(error);
  }
}

function normalizeGeneratedObject<ObjectType extends object>(object: ObjectType): ObjectType {
  // One wrapper per method, memoized. The trap fires on every FFI call, so
  // building a fresh closure each time would allocate per call and make each
  // call site megamorphic, defeating Hermes' inline caches.
  const wrappers = new Map<PropertyKey, unknown>();
  return new Proxy(object, {
    get(target, property, receiver) {
      const cached = wrappers.get(property);
      if (cached !== undefined) {
        return cached;
      }

      const value = Reflect.get(target, property, receiver) as unknown;
      if (typeof value !== "function") {
        return value;
      }

      const wrapper = (...args: unknown[]) => {
        return callGenerated(() => {
          const result = value.apply(target, args) as unknown;
          if (result instanceof Promise) {
            return result.catch((error: unknown) => {
              throw normalizeJazzRnError(error);
            });
          }
          return result;
        });
      };
      wrappers.set(property, wrapper);
      return wrapper;
    },
  });
}

function generatedQuery(query: PreparedQuery): GeneratedPreparedQuery {
  return query as GeneratedPreparedQuery;
}

function generatedAttachment(attachment: unknown): GeneratedQueryAttachment {
  return attachment as GeneratedQueryAttachment;
}

class RnSubscriptionShim implements Subscription {
  private readonly subscription: GeneratedSubscription;

  constructor(subscription: GeneratedSubscription) {
    this.subscription = normalizeGeneratedObject(subscription);
  }

  readAll(): unknown[] {
    return this.subscription.readAll().map(subscriptionEvent);
  }

  drain(): unknown[] {
    return this.subscription.drain().map(subscriptionEvent);
  }

  close(): boolean {
    return this.subscription.close();
  }
}

class RnTransportShim implements Transport {
  private readonly transport: GeneratedTransport;

  constructor(transport: GeneratedTransport) {
    this.transport = normalizeGeneratedObject(transport);
  }

  close(): boolean {
    return this.transport.close();
  }

  recvWireFrames(): Uint8Array[] {
    return this.transport.recvWireFrames().map(toUint8Array);
  }

  sendWireFrame(frame: Uint8Array): void {
    this.transport.sendWireFrame(toArrayBuffer(frame));
  }

  sendWireFrames(frames: readonly Uint8Array[]): void {
    this.transport.sendWireFrames(frames.map(toArrayBuffer));
  }

  tick(): number {
    return this.transport.tick();
  }
}

class RnWriteShim implements Write {
  private readonly write: GeneratedWrite;

  constructor(write: GeneratedWrite) {
    this.write = normalizeGeneratedObject(write);
  }

  get payload(): Uint8Array {
    return toUint8Array(this.write.payload());
  }

  wait(tier: string): void {
    this.write.wait(tier);
  }

  writeState(): unknown {
    return parseJson(this.write.writeState());
  }

  nextWriteStateChange(): Promise<void> {
    const waiter = this.write.registerWriteStateWaiter();
    return waiter.wait();
  }

  close(): boolean {
    return this.write.close();
  }
}

class RnTxShim implements Tx {
  readonly transaction: GeneratedTx;

  constructor(transaction: GeneratedTx) {
    this.transaction = normalizeGeneratedObject(transaction);
  }

  commit(): Write {
    return new RnWriteShim(this.transaction.commit());
  }

  rollback(): void {
    this.transaction.rollback();
  }

  /** Shared marshalling for the staged encoded writes; see {@link RnDbShim}. */
  private encodedWrite(
    method: "insertWithIdEncoded" | "restoreEncoded" | "updateEncoded" | "upsertEncoded",
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void {
    this.transaction[method](
      table,
      toArrayBuffer(rowId),
      toArrayBuffer(cells),
      optionalNumber(updatedAtMs),
    );
  }

  insertWithIdEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void {
    this.encodedWrite("insertWithIdEncoded", table, rowId, cells, updatedAtMs);
  }

  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void {
    this.encodedWrite("restoreEncoded", table, rowId, cells, updatedAtMs);
  }

  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    updatedAtMs?: number | null,
  ): void {
    this.encodedWrite("updateEncoded", table, rowId, patch, updatedAtMs);
  }

  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void {
    this.encodedWrite("upsertEncoded", table, rowId, cells, updatedAtMs);
  }

  delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): void {
    this.transaction.delete_(table, toArrayBuffer(rowId), optionalNumber(updatedAtMs));
  }
}

/** @internal Structural adapter from generated UniFFI objects to NativeRuntimeAdapter. */
export class RnDbShim implements NativeDb {
  private readonly db: GeneratedDb;

  constructor(db: GeneratedDb) {
    this.db = normalizeGeneratedObject(db);
  }

  static forModule(module: JazzRnModule): NativeDbConstructor {
    return {
      openMemory: (schema, config) =>
        new RnDbShim(
          callGenerated(() => module.RnDb.openMemory(toArrayBuffer(schema), toArrayBuffer(config))),
        ),
      openPersistent: (dataPath, schema, config) =>
        new RnDbShim(
          callGenerated(() =>
            module.RnDb.openPersistent(dataPath, toArrayBuffer(schema), toArrayBuffer(config)),
          ),
        ),
    };
  }

  all(query: PreparedQuery, opts: unknown): Uint8Array {
    return toUint8Array(this.db.all(generatedQuery(query), jsonArgument(opts)));
  }

  allForIdentity(query: PreparedQuery, author: Uint8Array, opts: unknown): Uint8Array {
    return toUint8Array(
      this.db.allForIdentity(generatedQuery(query), toArrayBuffer(author), jsonArgument(opts)),
    );
  }

  allRelationQuery(queryJson: string, opts: unknown): Uint8Array {
    return toUint8Array(this.db.allRelationQuery(queryJson, jsonArgument(opts)));
  }

  allRelationQueryForIdentity(queryJson: string, author: Uint8Array, opts: unknown): Uint8Array {
    return toUint8Array(
      this.db.allRelationQueryForIdentity(queryJson, toArrayBuffer(author), jsonArgument(opts)),
    );
  }

  allRelationSnapshot(query: PreparedQuery, opts: unknown): Uint8Array {
    return toUint8Array(this.db.allRelationSnapshot(generatedQuery(query), jsonArgument(opts)));
  }

  allRelationSnapshotForIdentity(
    query: PreparedQuery,
    author: Uint8Array,
    opts: unknown,
  ): Uint8Array {
    return toUint8Array(
      this.db.allRelationSnapshotForIdentity(
        generatedQuery(query),
        toArrayBuffer(author),
        jsonArgument(opts),
      ),
    );
  }

  setIdentityClaims(author: Uint8Array, claims: Record<string, unknown> | null | undefined): void {
    this.db.setIdentityClaims(toArrayBuffer(author), jsonArgument(claims));
  }

  attachQuery(query: PreparedQuery, opts: unknown): unknown {
    return this.db.attachQuery(generatedQuery(query), jsonArgument(opts));
  }

  attachQueryForIdentity(query: PreparedQuery, author: Uint8Array, opts: unknown): unknown {
    return this.db.attachQueryForIdentity(
      generatedQuery(query),
      toArrayBuffer(author),
      jsonArgument(opts),
    );
  }

  queryAttachmentIsCovered(attachment: unknown): boolean {
    return this.db.queryAttachmentIsCovered(generatedAttachment(attachment));
  }

  detachQuery(attachment: unknown): void {
    this.db.detachQuery(generatedAttachment(attachment));
  }

  prepareQuery(query: Uint8Array): PreparedQuery {
    return this.db.prepareQuery(toArrayBuffer(query));
  }

  subscribe(query: PreparedQuery, opts: unknown): Subscription {
    return new RnSubscriptionShim(this.db.subscribe(generatedQuery(query), jsonArgument(opts)));
  }

  subscribeForIdentity(query: PreparedQuery, author: Uint8Array, opts: unknown): Subscription {
    return new RnSubscriptionShim(
      this.db.subscribeForIdentity(
        generatedQuery(query),
        toArrayBuffer(author),
        jsonArgument(opts),
      ),
    );
  }

  subscribeRelationQuery(queryJson: string, opts: unknown): Subscription {
    return new RnSubscriptionShim(this.db.subscribeRelationQuery(queryJson, jsonArgument(opts)));
  }

  subscribeRelationQueryForIdentity(
    queryJson: string,
    author: Uint8Array,
    opts: unknown,
  ): Subscription {
    return new RnSubscriptionShim(
      this.db.subscribeRelationQueryForIdentity(
        queryJson,
        toArrayBuffer(author),
        jsonArgument(opts),
      ),
    );
  }

  /**
   * The encoded write calls differ only in which generated method they reach,
   * so they share one marshalling body. Naming the method through a union
   * keeps each call checked against `GeneratedDb`.
   */
  private encodedWrite(
    method: "insertWithIdEncoded" | "restoreEncoded" | "updateEncoded" | "upsertEncoded",
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return new RnWriteShim(
      this.db[method](
        table,
        toArrayBuffer(rowId),
        toArrayBuffer(cells),
        optionalNumber(updatedAtMs),
      ),
    );
  }

  private encodedWriteForIdentity(
    method:
      | "insertWithIdEncodedForIdentity"
      | "restoreEncodedForIdentity"
      | "updateEncodedForIdentity"
      | "upsertEncodedForIdentity",
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return new RnWriteShim(
      this.db[method](
        table,
        toArrayBuffer(rowId),
        toArrayBuffer(cells),
        toArrayBuffer(author),
        optionalNumber(updatedAtMs),
      ),
    );
  }

  insertWithIdEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return this.encodedWrite("insertWithIdEncoded", table, rowId, cells, updatedAtMs);
  }

  insertWithIdEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return this.encodedWriteForIdentity(
      "insertWithIdEncodedForIdentity",
      table,
      rowId,
      cells,
      author,
      updatedAtMs,
    );
  }

  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return this.encodedWrite("restoreEncoded", table, rowId, cells, updatedAtMs);
  }

  restoreEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return this.encodedWriteForIdentity(
      "restoreEncodedForIdentity",
      table,
      rowId,
      cells,
      author,
      updatedAtMs,
    );
  }

  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return this.encodedWrite("updateEncoded", table, rowId, patch, updatedAtMs);
  }

  updateEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return this.encodedWriteForIdentity(
      "updateEncodedForIdentity",
      table,
      rowId,
      patch,
      author,
      updatedAtMs,
    );
  }

  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return this.encodedWrite("upsertEncoded", table, rowId, cells, updatedAtMs);
  }

  upsertEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return this.encodedWriteForIdentity(
      "upsertEncodedForIdentity",
      table,
      rowId,
      cells,
      author,
      updatedAtMs,
    );
  }

  delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): Write {
    return new RnWriteShim(
      this.db.delete_(table, toArrayBuffer(rowId), optionalNumber(updatedAtMs)),
    );
  }

  deleteForIdentity(
    table: string,
    rowId: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write {
    return new RnWriteShim(
      this.db.deleteForIdentity(
        table,
        toArrayBuffer(rowId),
        toArrayBuffer(author),
        optionalNumber(updatedAtMs),
      ),
    );
  }

  canInsertEncoded(table: string, cells: Uint8Array): boolean {
    return this.db.canInsertEncoded(table, toArrayBuffer(cells));
  }

  canInsertEncodedForIdentity(table: string, cells: Uint8Array, author: Uint8Array): boolean {
    return this.db.canInsertEncodedForIdentity(table, toArrayBuffer(cells), toArrayBuffer(author));
  }

  canReadForIdentity(table: string, rowId: Uint8Array, author: Uint8Array): boolean {
    return this.db.canReadForIdentity(table, toArrayBuffer(rowId), toArrayBuffer(author));
  }

  canUpdateEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    author: Uint8Array,
  ): boolean {
    return this.db.canUpdateEncodedForIdentity(
      table,
      toArrayBuffer(rowId),
      toArrayBuffer(patch),
      toArrayBuffer(author),
    );
  }

  canDeleteForIdentity(table: string, rowId: Uint8Array, author: Uint8Array): boolean {
    return this.db.canDeleteForIdentity(table, toArrayBuffer(rowId), toArrayBuffer(author));
  }

  mergeableTx(): Tx {
    return new RnTxShim(this.db.mergeableTx());
  }

  mergeableTxForIdentity(author: Uint8Array): Tx {
    return new RnTxShim(this.db.mergeableTxForIdentity(toArrayBuffer(author)));
  }

  exclusiveTx(): Tx {
    return new RnTxShim(this.db.exclusiveTx());
  }

  allInTransaction(query: PreparedQuery, tx: Tx, opts: unknown): Uint8Array {
    return toUint8Array(
      this.db.allInTransaction(
        generatedQuery(query),
        (tx as RnTxShim).transaction,
        jsonArgument(opts),
      ),
    );
  }

  allInTransactionForIdentity(
    query: PreparedQuery,
    tx: Tx,
    author: Uint8Array,
    opts: unknown,
  ): Uint8Array {
    return toUint8Array(
      this.db.allInTransactionForIdentity(
        generatedQuery(query),
        (tx as RnTxShim).transaction,
        toArrayBuffer(author),
        jsonArgument(opts),
      ),
    );
  }

  localCurrentRow(table: string, rowId: Uint8Array): Uint8Array {
    return toUint8Array(this.db.localCurrentRow(table, toArrayBuffer(rowId)));
  }

  setTickScheduler(
    callback:
      | ((urgency: "immediate" | "deferred") => void)
      | ((error: Error | null, urgency: string) => void),
  ): void {
    this.db.setTickScheduler({
      onTickNeeded: (urgency) => {
        (callback as (urgency: string) => void)(urgency);
      },
    });
  }

  connectUpstream(): Transport {
    return new RnTransportShim(this.db.connectUpstream());
  }

  tick(): void {
    this.db.tick();
  }

  close(): void {
    this.db.close();
  }
}
