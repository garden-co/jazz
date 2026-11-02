/**
 * A runtime with no local engine (SPEC 8.14). The server evaluates every
 * query under this connection's session and sends finished results; writes
 * run on the server as that session. It backs the ordinary `Db` facade, so
 * the query and write API are the same as a local-first client's.
 *
 * Transactions, branch and snapshot views, and large-value updates and
 * streams run on the server too: their options travel as the native
 * bindings take them, and the server binds every write and read to the
 * session. Not supported: writes while offline beyond the reconnect that
 * follows, and backend attribution.
 */
import type {
  InsertValues,
  RuntimeSubscriptionDelta,
  Value,
  WasmSchema,
} from "../drivers/types.js";
import type {
  InsertResult,
  MutationErrorEvent,
  MutationResult,
  OpenTransactionId,
  Runtime,
  RuntimeWriteWaitOptions,
  StreamingInsertResult,
  StreamingMutationKind,
  StreamingValueSource,
  TransactionKind,
  TxId,
} from "../runtime/client.js";
import { parseSnapshotRef, type SnapshotRef } from "../runtime/snapshot-ref.js";
import { streamingChunks } from "../runtime/streaming-source.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import { writeRow } from "../runtime/write-row.js";
import {
  PostcardReader,
  readNativeSubscriptionDelta,
} from "../runtime/native-runtime/native-codec.js";
import {
  addNestedOuterColumns,
  branchViewFromWriteContext,
  decodeSubscriptionDelta,
  effectiveUpdatedAtMs,
  encodeCellsForPatch,
  encodeCellsForRow,
  encodeCellsForStreamingPatch,
  encodeCellsForStreamingRow,
  encodeQueryJson,
  formatUuid,
  openTransactionIdFromOptions,
  parseUuid,
  queryHasArraySubqueries,
  queryIncludesDeleted,
  readOptions,
  readRelationSnapshot,
  readRowBatches,
  rowsFromBatches,
  rowsFromRelationSnapshot,
  subscriptionOutputColumns,
  txIdFromContext,
  type EncodedBranchView,
  type SubscriptionOutputColumns,
} from "../runtime/native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
import {
  WebSocketCarrier,
  peerIdentityForWebSocketAuth,
  wireAuthFailureReason,
  type WebSocketConstructor,
  type WireError,
} from "../runtime/native-runtime/websocket.js";
import {
  FEATURE_THIN_RESULTS,
  decodeThinResponse,
  encodeThinRequest,
  isThinResponse,
  type ThinBody,
  type ThinOp,
} from "./wire.js";

export interface ThinRuntimeOptions {
  /** WebSocket implementation; the global one by default. */
  WebSocket?: WebSocketConstructor;
  /** Delays between reconnect attempts, in ms; the last one repeats. */
  reconnectDelaysMs?: readonly number[];
  /**
   * The canonical author this client acts as, with its account. The server
   * admits a public session only for the account its credential is assigned to.
   */
  author?: string;
}

/** A request failed on the server. `code` is the engine's stable error code. */
export class ThinRemoteError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "ThinRemoteError";
  }
}

/** The connection ended before the server answered. A write may or may not have committed. */
export class ThinConnectionLostError extends Error {
  constructor(message = "the connection to the Jazz server ended before it answered") {
    super(message);
    this.name = "ThinConnectionLostError";
  }
}

const DEFAULT_RECONNECT_DELAYS_MS = [250, 500, 1_000, 2_000, 5_000];

type OneShot = {
  op: ThinOp;
  resolve(body: ThinBody): void;
  reject(error: Error): void;
  sent: boolean;
};

type LiveQuery = {
  /** The runtime query as the client gave it, for {@link ThinRuntime.liveQueryJsons}. */
  queryJson: string;
  query: Uint8Array;
  /** Read options, as the native bindings take them. */
  options: object;
  outputColumns: SubscriptionOutputColumns;
  callback?: (result: RuntimeSubscriptionDelta | Error) => void;
  /** The request id it was last opened under, while open on the server. */
  requestId?: number;
  /** Results were delivered before a reconnect: the next opening replaces them. */
  delivered: boolean;
  resetNext: boolean;
};

type OpenTransaction = {
  kind: TransactionKind;
  /** The begin and every staged write: the commit waits for their answers. */
  requests: Promise<ThinBody>[];
  staged: boolean;
};

type Write = {
  done: Promise<void>;
  /** Some wait took the outcome; a failure needs no fallback report. */
  observed: boolean;
};

export class ThinRuntime implements Runtime {
  readonly schema: WasmSchema;
  private readonly schemaBytes: Uint8Array;
  private readonly WebSocketImpl?: WebSocketConstructor;
  private readonly reconnectDelaysMs: readonly number[];
  private readonly author: string | undefined;
  private carrier?: WebSocketCarrier;
  private url?: string;
  private authJson?: string;
  /** Requests and live queries go out only once the schema is bound. */
  private bound = false;
  private wanted = false;
  private closed = false;
  private attempt = 0;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private nextRequestId = 1;
  private readonly oneShots = new Map<number, OneShot>();
  private nextHandle = 1;
  private readonly liveQueries = new Map<number, LiveQuery>();
  private readonly liveByRequest = new Map<number, number>();
  private readonly writes = new Map<string, Write>();
  private readonly transactions = new Map<OpenTransactionId, OpenTransaction>();
  private mutationErrorCallback?: (event: MutationErrorEvent) => void;
  private authFailureCallback?: (reason: string) => void;

  constructor(schema: WasmSchema, options: ThinRuntimeOptions = {}) {
    this.schema = schema;
    this.schemaBytes = encodeSchema(schema);
    this.WebSocketImpl = options.WebSocket;
    this.reconnectDelaysMs = options.reconnectDelaysMs ?? DEFAULT_RECONNECT_DELAYS_MS;
    this.author = options.author;
  }

  // ---- connection -------------------------------------------------------

  connect(url: string, authJson: string): void {
    this.url = url;
    this.authJson = authJson;
    this.wanted = true;
    this.open();
  }

  async disconnect(options?: { rejectWaiters?: boolean }): Promise<void> {
    this.wanted = false;
    this.dropConnection(
      options?.rejectWaiters === false ? undefined : new ThinConnectionLostError(),
    );
  }

  updateAuth(authJson: string): void {
    this.authJson = authJson;
    if (!this.wanted) return;
    // The server binds a connection to the credential it opened with: a new
    // one takes a new connection.
    this.dropConnection(new ThinConnectionLostError());
    this.open();
  }

  onAuthFailure(callback: (reason: string) => void): void {
    this.authFailureCallback = callback;
  }

  onMutationError(callback: (event: MutationErrorEvent) => void): void {
    this.mutationErrorCallback = callback;
  }

  close(): void {
    this.closed = true;
    this.wanted = false;
    this.dropConnection(new ThinConnectionLostError("the thin runtime is closed"));
    for (const live of this.liveQueries.values()) live.callback = undefined;
    this.liveQueries.clear();
  }

  private open(): void {
    if (this.closed || !this.url || this.authJson === undefined || this.carrier) return;
    clearTimeout(this.reconnectTimer);
    const carrier = new WebSocketCarrier({
      endpointUrl: this.url,
      peerIdentity: peerIdentityForWebSocketAuth(
        this.authJson,
        this.author === undefined ? runtimeRandomBytes(16) : new TextEncoder().encode(this.author),
      ),
      features: FEATURE_THIN_RESULTS,
      authJson: this.authJson,
      WebSocket: this.WebSocketImpl,
      onFrame: (frame) => this.receive(frame),
      onTerminal: (error) => this.lost(carrier, error),
    });
    this.carrier = carrier;
    void carrier.ready().then(
      (negotiation) => {
        if (this.carrier !== carrier) return;
        if ((negotiation.features & FEATURE_THIN_RESULTS) === 0) {
          this.fatal(new Error("this Jazz server does not serve thin clients"));
          return;
        }
        this.bindSchema(carrier);
      },
      () => {
        // `onTerminal` reports why.
      },
    );
  }

  private bindSchema(carrier: WebSocketCarrier): void {
    const id = this.nextRequestId++;
    this.oneShots.set(id, {
      op: { type: "schema", schema: this.schemaBytes },
      sent: true,
      resolve: (body) => {
        if (this.carrier !== carrier) return;
        if (body.type === "error") {
          this.fatal(new ThinRemoteError(body.code, body.message));
          return;
        }
        this.attempt = 0;
        this.bound = true;
        for (const live of this.liveQueries.values()) if (live.callback) this.openLive(live);
        for (const [requestId, oneShot] of this.oneShots) {
          if (!oneShot.sent) this.transmit(requestId, oneShot);
        }
      },
      // A connection that ends before the answer is retried as a whole.
      reject: () => undefined,
    });
    void carrier.send(encodeThinRequest(id, { type: "schema", schema: this.schemaBytes }));
  }

  private lost(carrier: WebSocketCarrier, error: WireError): void {
    if (this.carrier !== carrier) return;
    this.dropConnection(new ThinConnectionLostError(`${error.code}: ${error.message}`));
    const authFailure = wireAuthFailureReason(error);
    if (authFailure) {
      // Nothing reconnects until the credential is renewed (`updateAuth`).
      this.authFailureCallback?.(authFailure);
      return;
    }
    if (!this.wanted || this.closed || error.retry === "never") return;
    const delays = this.reconnectDelaysMs;
    const delay = delays[Math.min(this.attempt, delays.length - 1)] ?? 1_000;
    this.attempt += 1;
    this.reconnectTimer = setTimeout(() => this.open(), delay);
  }

  /** Close the connection; requests already sent fail with `error`, live queries reopen later. */
  private dropConnection(error: Error | undefined): void {
    clearTimeout(this.reconnectTimer);
    const carrier = this.carrier;
    this.carrier = undefined;
    this.bound = false;
    carrier?.close();
    for (const live of this.liveQueries.values()) {
      if (live.requestId !== undefined) this.liveByRequest.delete(live.requestId);
      live.requestId = undefined;
      if (live.delivered) live.resetNext = true;
    }
    for (const [id, oneShot] of this.oneShots) {
      // Requests not sent yet wait for the next connection.
      if (!oneShot.sent) continue;
      this.oneShots.delete(id);
      if (error) oneShot.reject(error);
    }
  }

  private fatal(error: Error): void {
    this.wanted = false;
    this.dropConnection(error);
    for (const live of this.liveQueries.values()) live.callback?.(error);
  }

  // ---- requests ---------------------------------------------------------

  private request(op: ThinOp, id = this.nextRequestId++): Promise<ThinBody> {
    if (this.closed) return Promise.reject(new Error("the thin runtime is closed"));
    return new Promise<ThinBody>((resolve, reject) => {
      const oneShot: OneShot = { op, resolve, reject, sent: false };
      this.oneShots.set(id, oneShot);
      if (this.bound) this.transmit(id, oneShot);
    }).then((body) => {
      if (body.type === "error") throw new ThinRemoteError(body.code, body.message);
      return body;
    });
  }

  private transmit(id: number, oneShot: OneShot): void {
    oneShot.sent = true;
    void this.carrier?.send(encodeThinRequest(id, oneShot.op));
  }

  private receive(frame: Uint8Array): void {
    if (!isThinResponse(frame)) return;
    const { id, body } = decodeThinResponse(frame);
    const handle = this.liveByRequest.get(id);
    if (handle !== undefined) {
      this.receiveLive(handle, id, body);
      return;
    }
    const oneShot = this.oneShots.get(id);
    if (!oneShot) return;
    this.oneShots.delete(id);
    oneShot.resolve(body);
  }

  // ---- reads ------------------------------------------------------------

  async query(
    queryJson: string,
    _sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    const coreQueryJson = addNestedOuterColumns(queryJson);
    const transaction = openTransactionIdFromOptions(optionsJson);
    if (transaction) this.openTransaction(transaction, "Query setup");
    const body = await this.request({
      type: "query",
      query: encodeQueryJson(coreQueryJson, this.schema),
      options: readOptions(tier, queryIncludesDeleted(coreQueryJson), optionsJson) as object,
      transaction,
    });
    if (body.type !== "rows") throw new Error(`unexpected answer to a query: ${body.type}`);
    const projected = subscriptionOutputColumns(coreQueryJson, this.schema).rootColumns;
    return queryHasArraySubqueries(coreQueryJson) || body.relation
      ? rowsFromRelationSnapshot(readRelationSnapshot(body.rows), this.schema, projected)
      : rowsFromBatches(readRowBatches(body.rows), this.schema, projected);
  }

  createSubscription(
    queryJson: string,
    _sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    const handle = this.nextHandle++;
    this.liveQueries.set(handle, {
      queryJson,
      query: encodeQueryJson(queryJson, this.schema),
      options: readOptions(tier, queryIncludesDeleted(queryJson), optionsJson) as object,
      outputColumns: subscriptionOutputColumns(queryJson, this.schema),
      delivered: false,
      resetNext: false,
    });
    return handle;
  }

  executeSubscription(
    handle: number,
    onUpdate: (result: RuntimeSubscriptionDelta | Error) => void,
  ): void {
    const live = this.liveQueries.get(handle);
    if (!live) return;
    if (live.callback) throw new Error("Subscription has already been activated");
    live.callback = onUpdate;
    if (this.bound) this.openLive(live);
  }

  /** @internal The runtime queries the app is subscribed to now, for an upgrade to warm. */
  liveQueryJsons(): string[] {
    return [...this.liveQueries.values()]
      .filter((live) => live.callback !== undefined)
      .map((live) => live.queryJson);
  }

  unsubscribe(handle: number): void {
    const live = this.liveQueries.get(handle);
    if (!live) return;
    this.liveQueries.delete(handle);
    live.callback = undefined;
    if (live.requestId === undefined) return;
    this.liveByRequest.delete(live.requestId);
    if (this.bound) {
      void this.request({ type: "unsubscribe", subscription: live.requestId }).catch(() => {
        // The connection ended: the server closed the live query with it.
      });
    }
  }

  private openLive(live: LiveQuery): void {
    const id = this.nextRequestId++;
    const handle = [...this.liveQueries].find(([, candidate]) => candidate === live)?.[0];
    if (handle === undefined) return;
    live.requestId = id;
    this.liveByRequest.set(id, handle);
    void this.carrier?.send(
      encodeThinRequest(id, {
        type: "subscribe",
        query: live.query,
        options: live.options,
      }),
    );
  }

  private receiveLive(handle: number, requestId: number, body: ThinBody): void {
    const live = this.liveQueries.get(handle);
    if (!live?.callback) return;
    if (body.type === "event") {
      const reset = body.reset || live.resetNext;
      live.resetNext = false;
      live.delivered = true;
      const delta = decodeSubscriptionDelta(
        readNativeSubscriptionDelta(new PostcardReader(body.delta)),
        this.schema,
        reset,
        live.outputColumns,
      );
      live.callback(delta);
      return;
    }
    // Anything else ends the live query on the server.
    this.liveByRequest.delete(requestId);
    live.requestId = undefined;
    if (body.type === "error") {
      const callback = live.callback;
      this.liveQueries.delete(handle);
      live.callback = undefined;
      callback(new ThinRemoteError(body.code, body.message));
    }
  }

  // ---- writes -----------------------------------------------------------

  insert(
    table: string,
    values: InsertValues,
    writeContext?: string | null,
    objectId?: string | null,
  ): InsertResult {
    const context = writeTarget(writeContext, "Insert");
    const definition = this.definition(table);
    const id = objectId ?? formatUuid(uuidv7());
    const cells = encodeCellsForRow(definition, values, table);
    const receipt = this.mutate(context, {
      type: "insert",
      table,
      cells,
      options: {
        rowId: parseUuid(id),
        branch: context.branchView?.head,
        updatedAtMs: context.updatedAtMs,
      },
    });
    const row = writeRow(table, id, definition.columns, values);
    return { id: row.id, values: row.values, ...receipt };
  }

  update(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    writeContext?: string | null,
  ): MutationResult {
    const context = writeTarget(writeContext, "Update");
    return this.mutate(context, {
      type: "update",
      table,
      row: parseUuid(objectId),
      patch: encodeCellsForPatch(this.definition(table), values),
      options: viewOptions(context),
    });
  }

  upsert(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): MutationResult {
    const context = writeTarget(writeContext, "Upsert");
    return this.mutate(context, {
      type: "upsert",
      table,
      row: parseUuid(objectId),
      cells: encodeCellsForPatch(this.definition(table), values),
      options: viewOptions(context),
    });
  }

  delete(table: string, objectId: string, writeContext?: string | null): MutationResult {
    const context = writeTarget(writeContext, "Delete");
    this.definition(table);
    return this.mutate(context, {
      type: "delete",
      table,
      row: parseUuid(objectId),
      options: viewOptions(context),
    });
  }

  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): InsertResult {
    const context = writeTarget(writeContext, "Restore");
    const definition = this.definition(table);
    const receipt = this.mutate(context, {
      type: "restore",
      table,
      row: parseUuid(objectId),
      cells: encodeCellsForRow(definition, values, table),
      options: { branch: context.branchView?.head, updatedAtMs: context.updatedAtMs },
    });
    const row = writeRow(table, objectId, definition.columns, values);
    return { id: row.id, values: row.values, ...receipt };
  }

  updateLargeValues(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    descriptors: readonly unknown[],
    writeContext?: string | null,
  ): MutationResult {
    const context = writeTarget(writeContext, "Update");
    // Branch views have no partial-value coordinate space yet (#2087).
    if (context.branchView) {
      throw new Error("Typed large-value updates are not supported in branch views.");
    }
    return this.mutate(context, {
      type: "updateLargeValues",
      table,
      row: parseUuid(objectId),
      patch: encodeCellsForPatch(this.definition(table), values),
      mutations: descriptors,
      updatedAtMs: context.updatedAtMs,
    });
  }

  async streamingMutation(
    mutation: StreamingMutationKind,
    table: string,
    values: InsertValues,
    column: string,
    source: StreamingValueSource,
    writeContext?: string | null,
    objectId?: string | null,
  ): Promise<StreamingInsertResult> {
    const operation =
      mutation === "insert" ? "Insert" : mutation === "update" ? "Update" : "Upsert";
    const context = writeTarget(writeContext, operation);
    const transaction = context.transaction
      ? this.openTransaction(context.transaction, operation)
      : undefined;
    const definition = this.definition(table);
    const kind = definition.columns.find((candidate) => candidate.name === column)?.column_type
      .type;
    if (kind !== "Text" && kind !== "Json" && kind !== "Bytea") {
      throw new Error(
        `Streaming insert requires a Text, Json, or Bytea column: ${table}.${column}`,
      );
    }
    const row = objectId ? parseUuid(objectId) : runtimeRandomBytes(16);
    const stream = this.nextRequestId++;
    const begun = this.request(
      {
        type: "beginStream",
        table,
        row,
        cells:
          mutation === "insert"
            ? encodeCellsForStreamingRow(definition, values, column, table)
            : encodeCellsForStreamingPatch(definition, values, column),
        column,
        mutation,
        options: viewOptions(context),
        transaction: context.transaction ?? undefined,
      },
      stream,
    );
    // Chunks go one window at a time: each waits for the server to apply
    // the one before, which bounds what a slow server buffers.
    const push = async (bytes: Uint8Array) => {
      const windowBytes = 64 * 1024;
      for (let offset = 0; offset < bytes.byteLength; offset += windowBytes) {
        await this.request({
          type: "streamChunk",
          stream,
          chunk: bytes.subarray(offset, offset + windowBytes),
        });
      }
    };
    const encoder = new TextEncoder();
    let pendingHighSurrogate = "";
    try {
      await begun;
      for await (const chunk of streamingChunks(source)) {
        if (typeof chunk === "string") {
          if (kind === "Bytea") throw new Error("Bytea streams require Uint8Array chunks");
          let text = pendingHighSurrogate + chunk;
          pendingHighSurrogate = "";
          const trailing = text.charCodeAt(text.length - 1);
          if (trailing >= 0xd800 && trailing <= 0xdbff) {
            pendingHighSurrogate = text.slice(-1);
            text = text.slice(0, -1);
          }
          if (text.length > 0) await push(encoder.encode(text));
        } else if (chunk instanceof Uint8Array) {
          if (pendingHighSurrogate) {
            await push(encoder.encode(pendingHighSurrogate));
            pendingHighSurrogate = "";
          }
          await push(chunk);
        } else {
          throw new Error("Streaming insert chunks must be strings or Uint8Array values");
        }
      }
      if (pendingHighSurrogate) await push(encoder.encode(pendingHighSurrogate));
    } catch (error) {
      void this.request({ type: "abortStream", stream }).catch(() => undefined);
      throw error;
    }
    if (transaction) {
      // The value is staged in the transaction; its commit reports a
      // refusal of the row write.
      const staged = this.request({ type: "finishStream", stream });
      transaction.requests.push(staged);
      transaction.staged = true;
      await staged;
      return { id: formatUuid(row), kind: "staged", openTransactionId: context.transaction! };
    }
    return {
      id: formatUuid(row),
      kind: "committed",
      txId: this.write({ type: "finishStream", stream }),
    };
  }

  // ---- transactions -----------------------------------------------------

  beginTransaction(
    kind: TransactionKind,
    id: OpenTransactionId,
    _sessionJson?: string | null,
  ): OpenTransactionId {
    if (this.closed) throw new Error("Begin transaction failed: the thin runtime is closed");
    if (this.transactions.has(id)) {
      throw new Error(`Begin transaction failed: transaction ${id} has already been opened`);
    }
    // The server binds the transaction to this connection's session.
    const begun = this.request({ type: "begin", transaction: id, kind });
    begun.catch(() => undefined);
    this.transactions.set(id, { kind, requests: [begun], staged: false });
    return id;
  }

  commitTransaction(id: OpenTransactionId): Promise<TxId> {
    const transaction = this.transactions.get(id);
    if (!transaction) {
      throw new Error(
        `Commit transaction failed: Write error: open transaction ${id} has already been completed or was never opened`,
      );
    }
    if (!transaction.staged && transaction.kind === "mergeable") {
      throw new Error(
        "Commit transaction failed: empty mergeable transaction has no committed unit; roll it back instead",
      );
    }
    this.transactions.delete(id);
    // A staged write the server refused outright fails the commit; one its
    // row's state refused is the commit's own rejection, from the server.
    return Promise.all(transaction.requests).then(
      () => this.write({ type: "commit", transaction: id }),
      (error: unknown) => {
        void this.request({ type: "rollback", transaction: id }).catch(() => undefined);
        throw error;
      },
    );
  }

  async rollbackTransaction(id: OpenTransactionId): Promise<boolean> {
    const transaction = this.transactions.get(id);
    if (!transaction) {
      throw new Error(
        `Rollback transaction failed: Write error: open transaction ${id} has already been completed or was never opened`,
      );
    }
    this.transactions.delete(id);
    await this.request({ type: "rollback", transaction: id });
    return true;
  }

  async captureSnapshot(): Promise<SnapshotRef> {
    const body = await this.request({ type: "captureSnapshot" });
    if (body.type !== "snapshot") {
      throw new Error(`unexpected answer to a snapshot capture: ${body.type}`);
    }
    return parseSnapshotRef(new TextDecoder().decode(body.snapshot));
  }

  private openTransaction(id: OpenTransactionId, operation: string): OpenTransaction {
    const transaction = this.transactions.get(id);
    if (!transaction) {
      throw new Error(
        `${operation} failed: Write error: open transaction ${id} has already been completed or was never opened`,
      );
    }
    return transaction;
  }

  /** A write, staged in its transaction or committed on its own. */
  private mutate(
    context: WriteTarget,
    op: Extract<ThinOp, { transaction?: string }>,
  ): MutationResult {
    const id = context.transaction;
    if (!id) return { kind: "committed", txId: this.write(op) };
    const transaction = this.openTransaction(id, context.operation);
    const staged = this.request({ ...op, transaction: id } as ThinOp);
    staged.catch(() => undefined);
    transaction.requests.push(staged);
    transaction.staged = true;
    return { kind: "staged", openTransactionId: id };
  }

  async waitForTransaction(
    txId: TxId | Promise<TxId>,
    _tier: string,
    options?: RuntimeWriteWaitOptions,
  ): Promise<void> {
    const write = this.writes.get(await txId);
    if (!write) throw new Error(`unknown thin write ${String(await txId)}`);
    // The server confirms a thin write only once it committed on the
    // authority, which meets every durability tier.
    if (!options?.observeOnly) write.observed = true;
    if (!options?.signal) return write.done;
    const signal = options.signal;
    signal.throwIfAborted();
    return Promise.race([
      write.done,
      new Promise<never>((_, reject) =>
        signal.addEventListener("abort", () => reject(signal.reason), { once: true }),
      ),
    ]);
  }

  /** Every write made so far has been answered. Every write is global once answered. */
  async waitForPendingWrites(
    _tier?: string,
    options: { throughOutage?: boolean; signal?: AbortSignal } = {},
  ): Promise<void> {
    const answered = Promise.allSettled([...this.writes.values()].map((write) => write.done));
    const signal = options.signal;
    if (!signal) {
      await answered;
      return;
    }
    signal.throwIfAborted();
    await Promise.race([
      answered,
      new Promise<never>((_, reject) =>
        signal.addEventListener("abort", () => reject(signal.reason), { once: true }),
      ),
    ]);
  }

  private write(op: ThinOp): TxId {
    const txId = `thin:${this.nextRequestId}` as TxId;
    const write: Write = { observed: false, done: Promise.resolve() };
    write.done = this.request(op).then(
      (body) => {
        if (body.type !== "written") throw new Error(`unexpected answer to a write: ${body.type}`);
      },
      (error: unknown) => {
        const failure = error instanceof Error ? error : new Error(String(error));
        // A failure no wait observes is reported once, like a local-first
        // client's rejected write.
        queueMicrotask(() => {
          if (write.observed) return;
          this.mutationErrorCallback?.({
            code: failure instanceof ThinRemoteError ? failure.code : "connection_lost",
            reason: failure.message,
            transaction: {
              transactionId: txId,
              kind: "mergeable",
              sealed: true,
              latestSettlement: {
                kind: "rejected",
                transactionId: txId,
                code: failure instanceof ThinRemoteError ? failure.code : "connection_lost",
                reason: failure.message,
              },
            },
          });
        });
        throw failure;
      },
    );
    // An unobserved failure is reported above, not as an unhandled rejection.
    write.done.catch(() => undefined);
    this.writes.set(txId, write);
    return txId;
  }

  private definition(table: string): (typeof this.schema)[string] {
    const definition = this.schema[table];
    if (!definition) throw new Error(`unknown table ${table}`);
    return definition;
  }
}

type WriteTarget = {
  operation: string;
  transaction?: OpenTransactionId;
  branchView?: EncodedBranchView;
  updatedAtMs: number;
};

/** What a write context asks of a thin write. Its session is the connection's. */
function writeTarget(writeContext: string | null | undefined, operation: string): WriteTarget {
  if (writeContext) {
    const parsed = JSON.parse(writeContext) as { attribution?: unknown };
    if (parsed.attribution != null) {
      throw new Error(`${operation} failed: attributed writes are not supported in thin mode`);
    }
  }
  return {
    operation,
    transaction: txIdFromContext(writeContext),
    branchView: branchViewFromWriteContext(writeContext),
    updatedAtMs: effectiveUpdatedAtMs(writeContext) ?? Date.now(),
  };
}

/** Write options for a write through a branch view. */
function viewOptions(context: WriteTarget): object {
  return {
    head: context.branchView?.head,
    base: context.branchView?.base,
    updatedAtMs: context.updatedAtMs,
  };
}

/** A UUIDv7: a millisecond timestamp, then random bits. */
function uuidv7(): Uint8Array {
  const bytes = runtimeRandomBytes(16);
  const now = Date.now();
  bytes[0] = Math.floor(now / 2 ** 40) & 0xff;
  bytes[1] = Math.floor(now / 2 ** 32) & 0xff;
  bytes[2] = Math.floor(now / 2 ** 24) & 0xff;
  bytes[3] = Math.floor(now / 2 ** 16) & 0xff;
  bytes[4] = Math.floor(now / 2 ** 8) & 0xff;
  bytes[5] = now & 0xff;
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  return bytes;
}
