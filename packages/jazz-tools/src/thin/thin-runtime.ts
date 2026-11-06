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
import { CONNECTED, CONNECTING, offlineLink, type LinkStatus } from "../runtime/sync-status.js";
import { streamingChunks } from "../runtime/streaming-source.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import { writeRow } from "../runtime/write-row.js";
import { bytesToHex, formatUuid } from "../runtime/hex.js";
import { canonicalAuthorSubject } from "../runtime/author-id.js";
import type { Session } from "../runtime/context.js";
import {
  PostcardReader,
  readNativeSubscriptionDelta,
} from "../runtime/native-runtime/native-codec.js";
import {
  addNestedOuterColumns,
  assertNoUnsupportedPermissionIntrospection,
  assertSupportedReadOptions,
  branchViewFromWriteContext,
  decodeSubscriptionDelta,
  effectiveUpdatedAtMs,
  encodeCellsForPatch,
  encodeCellsForRow,
  encodeCellsForStreamingPatch,
  encodeCellsForStreamingRow,
  encodeQueryJson,
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
  MAX_THIN_MESSAGE_BYTES,
  ThinAssembler,
  thinWireFrames,
  isThinResponse,
  type ThinBody,
  type ThinOp,
} from "./wire.js";

export interface ThinRuntimeOptions {
  /** WebSocket implementation; the global one by default. */
  WebSocket?: WebSocketConstructor;
  /** Delays between reconnect attempts, in ms; the last one repeats. */
  reconnectDelaysMs?: readonly number[];
  /** How long connecting may keep failing before the link reports an outage. */
  outageReportMs?: number;
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

/**
 * The server ends a thin connection's open transactions with it (on a
 * reconnect, including the one a credential renewal takes): their staged
 * writes are gone, and the transaction can only be started again.
 */
export class ThinTransactionLostError extends Error {
  constructor(id: OpenTransactionId) {
    super(
      `transaction ${id} was lost with the connection to the Jazz server; its staged writes were discarded, so start it again`,
    );
    this.name = "ThinTransactionLostError";
  }
}

const DEFAULT_RECONNECT_DELAYS_MS = [250, 500, 1_000, 2_000, 5_000];

type OneShot = {
  op: ThinOp;
  resolve(body: ThinBody): void;
  reject(error: Error): void;
  sent: boolean;
  /** A write: its fate is unknown once its connection ends unanswered. */
  write?: boolean;
  /** A write under a `writeKey`: sent again on the next connection when its
   * connection ends unanswered, since the server answers a resend with the
   * first one's outcome instead of writing again. */
  resendable?: boolean;
  /** Sent once and waiting to go out again (see `resendable`). */
  resending?: boolean;
};

/** Standalone writes the server dedups by their `writeKey` (SPEC 8 §8.14). */
const RESENDABLE_WRITES = new Set<ThinOp["type"]>([
  "insert",
  "update",
  "upsert",
  "delete",
  "restore",
]);

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
  /** Its begin, which reached the server once `sent`. */
  begin: OneShot;
  /** The connection it was begun on ended, and the server abandoned it. */
  lost: boolean;
};

type Write = {
  done: Promise<void>;
  /** Some wait took the outcome; a failure needs no fallback report. */
  observed: boolean;
  /** The server confirmed it. */
  committed: boolean;
  /** It settled: committed, rejected, or given up with its fate unknown. */
  answered: boolean;
};

/**
 * How long the connection may keep failing before the link reports an
 * outage, as the core link does: a short blip never shows as offline.
 */
const OUTAGE_REPORT_MS = 7_500;

/** Committed writes kept for later waits on them. */
const RETAINED_WRITES = 1024;

export class ThinRuntime implements Runtime {
  readonly schema: WasmSchema;
  private readonly schemaBytes: Uint8Array;
  private readonly WebSocketImpl?: WebSocketConstructor;
  private readonly reconnectDelaysMs: readonly number[];
  private author: string | undefined;
  private carrier?: WebSocketCarrier;
  /** Joins the current connection's split responses. */
  private parts = new ThinAssembler();
  private url?: string;
  private authJson?: string;
  /** Requests and live queries go out only once the schema is bound. */
  private bound = false;
  private wanted = false;
  private closed = false;
  private attempt = 0;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  /** The server refused the credential: nothing reconnects until `updateAuth`. */
  private refused = false;
  /** The connection ended for good (`fatal`, `close`). */
  private stopped = false;
  /** When connecting started failing; cleared once connected or refused. */
  private failingSince?: number;
  private outagePublished = false;
  private readonly outageReportMs: number;
  private nextRequestId = 1;
  private readonly oneShots = new Map<number, OneShot>();
  private nextHandle = 1;
  private readonly liveQueries = new Map<number, LiveQuery>();
  private readonly liveByRequest = new Map<number, number>();
  private readonly writes = new Map<string, Write>();
  /** This runtime's writer node: with {@link nextWriteSeq}, each write's
   * `writeKey`. The server commits the write under a node it derives from
   * this one and the session's author, so one node serves every identity
   * the runtime writes as. */
  private readonly writerNode = bytesToHex(runtimeRandomBytes(16));
  private nextWriteSeq = 1;
  /** Writes the server has not answered yet. */
  private unanswered = 0;
  private readonly syncStatusListeners = new Set<() => void>();
  private readonly transactions = new Map<OpenTransactionId, OpenTransaction>();
  private mutationErrorCallback?: (event: MutationErrorEvent) => void;
  private authFailureCallback?: (reason: string) => void;

  constructor(schema: WasmSchema, options: ThinRuntimeOptions = {}) {
    this.schema = schema;
    this.schemaBytes = encodeSchema(schema);
    this.WebSocketImpl = options.WebSocket;
    this.reconnectDelaysMs = options.reconnectDelaysMs ?? DEFAULT_RECONNECT_DELAYS_MS;
    this.outageReportMs = options.outageReportMs ?? OUTAGE_REPORT_MS;
    this.author = options.author;
  }

  // ---- connection -------------------------------------------------------

  connect(url: string, authJson: string): void {
    const changed =
      (this.url !== undefined && url !== this.url) ||
      (this.authJson !== undefined && authJson !== this.authJson);
    this.url = url;
    this.authJson = authJson;
    this.wanted = true;
    // Another server or credential, as for `updateAuth`: the connection
    // bound to the old one goes, and a sent write waiting to go out again
    // ends with its fate unknown instead of being made as someone else.
    if (changed) this.dropConnection(new ThinConnectionLostError());
    // An explicit connect (a reconnect after a refusal or a terminal error)
    // starts over, as the core link does.
    if (!this.closed) this.stopped = false;
    this.setRefused(false);
    this.open();
  }

  async disconnect(options?: { rejectWaiters?: boolean }): Promise<void> {
    this.wanted = false;
    this.clearOutage();
    this.dropConnection(
      options?.rejectWaiters === false ? undefined : new ThinConnectionLostError(),
    );
  }

  updateAuth(authJson: string): void {
    this.authJson = authJson;
    this.setRefused(false);
    // The new credential may name another user (a cookie session names
    // none here), as whom a sent write must not be made: one that may have
    // been made ends with its fate unknown, as it did before resends.
    this.dropConnection(new ThinConnectionLostError());
    if (!this.wanted) return;
    // The server binds a connection to the credential it opened with: a new
    // one takes a new connection.
    this.open();
  }

  /**
   * The client's own session: calls naming it are served as this
   * connection, calls naming any other are refused. Set before the client
   * connects and again as its auth changes.
   */
  useSession(session: Session | null): void {
    this.author = session
      ? canonicalAuthorSubject(session.issuer, session.user_id, session.account_id)
      : undefined;
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
    this.stop();
    this.dropConnection(new ThinConnectionLostError("the thin runtime is closed"));
    for (const live of this.liveQueries.values()) live.callback = undefined;
    this.liveQueries.clear();
  }

  private open(): void {
    if (this.closed || !this.url || this.authJson === undefined || this.carrier) return;
    clearTimeout(this.reconnectTimer);
    this.failingSince ??= Date.now();
    this.parts.reset();
    const carrier = new WebSocketCarrier({
      endpointUrl: this.url,
      peerIdentity: peerIdentityForWebSocketAuth(
        this.authJson,
        this.author === undefined ? runtimeRandomBytes(16) : new TextEncoder().encode(this.author),
      ),
      features: FEATURE_THIN_RESULTS,
      authJson: this.authJson,
      WebSocket: this.WebSocketImpl,
      onFrame: (frame) => this.receive(carrier, frame),
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
        this.refused = false;
        this.stopped = false;
        this.clearOutage();
        this.notifySyncStatus();
        for (const live of this.liveQueries.values()) if (live.callback) this.openLive(live);
        for (const [requestId, oneShot] of this.oneShots) {
          if (!oneShot.sent) this.transmit(requestId, oneShot);
        }
      },
      // A connection that ends before the answer is retried as a whole.
      reject: () => undefined,
    });
    this.sendFrame(
      carrier,
      encodeThinRequest(id, { type: "schema", schema: this.schemaBytes }),
      (bytes) => {
        this.oneShots.delete(id);
        this.fatal(tooLarge("schema", bytes));
      },
    );
  }

  /** Send `frame`, in parts when it is over a wire frame; `tooLarge`
   * instead past what one request takes. */
  private sendFrame(
    carrier: WebSocketCarrier | undefined,
    frame: Uint8Array,
    refused: (bytes: number) => void,
  ): boolean {
    const frames = thinWireFrames(frame);
    if (frames === null) {
      refused(frame.byteLength);
      return false;
    }
    // Parts of one frame go back to back.
    void carrier?.sendBatch(frames);
    return true;
  }

  private lost(carrier: WebSocketCarrier, error: WireError): void {
    if (this.carrier !== carrier) return;
    const lostError = new ThinConnectionLostError(`${error.code}: ${error.message}`);
    const authFailure = wireAuthFailureReason(error);
    if (authFailure) {
      // Refused before the drop publishes, so the link never reads
      // "connecting" in between.
      this.clearOutage();
      this.refused = true;
      // Writes left unanswered wait for the renewed credential's connection.
      this.dropConnection(lostError, this.wanted && !this.closed && error.retry !== "never");
      this.notifySyncStatus();
      // Nothing reconnects until the credential is renewed (`updateAuth`).
      this.authFailureCallback?.(authFailure);
      return;
    }
    if (error.retry === "never") {
      // Stopped for good, as `fatal`: only an explicit connect starts over.
      this.wanted = false;
      this.stopped = true;
      this.clearOutage();
    }
    this.dropConnection(lostError, this.wanted && !this.closed);
    // An attempt that fails past the window reports the outage.
    if (
      !this.stopped &&
      !this.outagePublished &&
      this.failingSince !== undefined &&
      Date.now() - this.failingSince >= this.outageReportMs
    ) {
      this.outagePublished = true;
    }
    this.notifySyncStatus();
    if (!this.wanted || this.closed || error.retry === "never") return;
    const delays = this.reconnectDelaysMs;
    const delay = delays[Math.min(this.attempt, delays.length - 1)] ?? 1_000;
    this.attempt += 1;
    this.reconnectTimer = setTimeout(() => this.open(), delay);
  }

  /** Close the connection; requests already sent fail with `error`, live queries reopen later.
   * With `resend`, a write under a `writeKey` the connection left unanswered
   * waits for the next connection instead, and goes out again in order. */
  private dropConnection(error: Error | undefined, resend = false): void {
    clearTimeout(this.reconnectTimer);
    const carrier = this.carrier;
    this.carrier = undefined;
    if (this.bound) {
      this.bound = false;
      this.notifySyncStatus();
    }
    carrier?.close();
    for (const live of this.liveQueries.values()) {
      if (live.requestId !== undefined) this.liveByRequest.delete(live.requestId);
      live.requestId = undefined;
      if (live.delivered) live.resetNext = true;
    }
    // The server abandoned every transaction begun on this connection.
    const lost = new Set<OpenTransactionId>();
    for (const [id, transaction] of this.transactions) {
      if (transaction.begin.sent && !transaction.lost) {
        // Kept, so its handle's commit says it was lost, but without its
        // staged writes' answers.
        transaction.lost = true;
        transaction.requests = [];
        lost.add(id);
      }
    }
    for (const [id, oneShot] of this.oneShots) {
      const transaction = transactionOf(oneShot.op);
      if (transaction !== undefined && lost.has(transaction)) {
        this.oneShots.delete(id);
        oneShot.reject(new ThinTransactionLostError(transaction));
        continue;
      }
      // Requests not sent yet wait for the next connection, and so does a
      // write sent once, unless this drop ends its resends.
      if (!oneShot.sent && !(oneShot.resending && !resend)) continue;
      if (oneShot.sent && resend && oneShot.resendable) {
        oneShot.sent = false;
        oneShot.resending = true;
        continue;
      }
      this.oneShots.delete(id);
      if (error) oneShot.reject(error);
      // A sent write is never sent again after an explicit disconnect: it may
      // have committed, so it ends with its fate unknown. (A sent read is
      // dropped unanswered, as before.)
      else if (oneShot.write) oneShot.reject(new ThinConnectionLostError());
    }
  }

  private fatal(error: Error): void {
    this.wanted = false;
    this.stop();
    this.dropConnection(error);
    for (const live of this.liveQueries.values()) live.callback?.(error);
  }

  // ---- requests ---------------------------------------------------------

  private request(op: ThinOp, id = this.nextRequestId++): Promise<ThinBody> {
    return this.requestWith(op, id).answer;
  }

  /** {@link request}, with the request's own record. */
  private requestWith(
    op: ThinOp,
    id = this.nextRequestId++,
  ): { answer: Promise<ThinBody>; oneShot?: OneShot } {
    if (this.closed)
      return {
        answer: Promise.reject(new Error("the thin runtime is closed")),
      };
    let oneShot: OneShot | undefined;
    const answer = new Promise<ThinBody>((resolve, reject) => {
      oneShot = { op, resolve, reject, sent: false };
      this.oneShots.set(id, oneShot);
      if (this.bound) this.transmit(id, oneShot);
    }).then((body) => {
      if (body.type === "error") throw new ThinRemoteError(body.code, body.message);
      return body;
    });
    return { answer, oneShot };
  }

  private transmit(id: number, oneShot: OneShot): void {
    const sent = this.sendFrame(this.carrier, encodeThinRequest(id, oneShot.op), (bytes) => {
      // Refused here: the server would refuse it.
      this.oneShots.delete(id);
      oneShot.reject(tooLarge(oneShot.op.type, bytes));
    });
    if (sent) oneShot.sent = true;
  }

  private receive(carrier: WebSocketCarrier, piece: Uint8Array): void {
    if (this.carrier !== carrier) return;
    let response: ReturnType<typeof decodeThinResponse>;
    try {
      let frame = piece;
      if (ThinAssembler.isPart(piece)) {
        const joined = this.parts.push(piece);
        if (joined === undefined) return;
        frame = joined;
      } else if (this.parts.joining) {
        throw new Error("a thin frame arrived inside a split one");
      }
      if (!isThinResponse(frame)) return;
      response = decodeThinResponse(frame);
    } catch (error) {
      // The server broke the framing: start over on a new connection.
      this.parts.reset();
      this.lost(carrier, {
        code: "MalformedFrame",
        retry: "later",
        message: error instanceof Error ? error.message : String(error),
      });
      return;
    }
    const { id, body } = response;
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

  /**
   * Every read and write runs as this connection's session. The client
   * names its own session with each call; another one (a trusted backend
   * acting for a user, `db.forSession(other)`) is refused rather than
   * silently served as this one. A connection that does not know its
   * author (a cookie session) refuses any session named explicitly.
   */
  private refuseOtherSession(sessionJson: string | null | undefined, operation: string): void {
    if (sessionJson == null) return;
    type SessionJson = {
      issuer?: string;
      user_id?: string;
      account_id?: string;
    };
    // A session, or a write context naming one.
    const parsed = JSON.parse(sessionJson) as SessionJson & {
      session?: SessionJson;
      attribution?: unknown;
    };
    if (parsed.attribution != null) {
      throw new Error(`${operation} failed: attributed writes are not supported in thin mode`);
    }
    const session = parsed.session ?? parsed;
    if (session.user_id === undefined) return;
    const author = session.issuer
      ? canonicalAuthorSubject(session.issuer, session.user_id, session.account_id)
      : undefined;
    if (this.author === undefined || author !== this.author) {
      throw new Error(
        `${operation} failed: reading or writing as another session is not supported in thin mode`,
      );
    }
  }

  /** What a write context asks of a thin write. Its session is the connection's. */
  private writeTarget(writeContext: string | null | undefined, operation: string): WriteTarget {
    this.refuseOtherSession(writeContext, operation);
    return {
      operation,
      transaction: txIdFromContext(writeContext),
      branchView: branchViewFromWriteContext(writeContext),
      updatedAtMs: effectiveUpdatedAtMs(writeContext) ?? Date.now(),
    };
  }

  async query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    this.refuseOtherSession(sessionJson, "Query");
    assertSupportedReadOptions(tier, optionsJson);
    assertNoUnsupportedPermissionIntrospection(queryJson);
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
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    this.refuseOtherSession(sessionJson, "Subscription");
    assertSupportedReadOptions(tier, optionsJson);
    assertNoUnsupportedPermissionIntrospection(queryJson);
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
      void this.request({
        type: "unsubscribe",
        subscription: live.requestId,
      }).catch(() => {
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
    this.sendFrame(
      this.carrier,
      encodeThinRequest(id, { type: "subscribe", query: live.query, options: live.options }),
      (bytes) => {
        this.liveByRequest.delete(id);
        live.requestId = undefined;
        this.receiveLive(handle, id, {
          type: "error",
          code: "TooLarge",
          message: tooLarge("subscribe", bytes).message,
        });
      },
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
    // Anything else ends the live query on the server. The app still wants
    // it (an unsubscribe forgets the request first), so it hears why.
    this.liveByRequest.delete(requestId);
    live.requestId = undefined;
    const callback = live.callback;
    this.liveQueries.delete(handle);
    live.callback = undefined;
    callback(
      body.type === "error"
        ? new ThinRemoteError(body.code, body.message)
        : new ThinRemoteError("Closed", "the Jazz server ended this live query"),
    );
  }

  // ---- writes -----------------------------------------------------------

  insert(
    table: string,
    values: InsertValues,
    writeContext?: string | null,
    objectId?: string | null,
  ): InsertResult {
    const context = this.writeTarget(writeContext, "Insert");
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
    const context = this.writeTarget(writeContext, "Update");
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
    const context = this.writeTarget(writeContext, "Upsert");
    return this.mutate(context, {
      type: "upsert",
      table,
      row: parseUuid(objectId),
      cells: encodeCellsForPatch(this.definition(table), values),
      options: viewOptions(context),
    });
  }

  delete(table: string, objectId: string, writeContext?: string | null): MutationResult {
    const context = this.writeTarget(writeContext, "Delete");
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
    const context = this.writeTarget(writeContext, "Restore");
    const definition = this.definition(table);
    const receipt = this.mutate(context, {
      type: "restore",
      table,
      row: parseUuid(objectId),
      cells: encodeCellsForRow(definition, values, table),
      options: {
        branch: context.branchView?.head,
        updatedAtMs: context.updatedAtMs,
      },
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
    const context = this.writeTarget(writeContext, "Update");
    // Branch views have no partial-value coordinate space yet (#2087).
    if (context.branchView) {
      throw new Error(
        "Partial-value updates (applyDiffs, editedFrom) are not yet supported in branch views.",
      );
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
    const context = this.writeTarget(writeContext, operation);
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
    const row = objectId ? parseUuid(objectId) : uuidv7();
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
      return {
        id: formatUuid(row),
        kind: "staged",
        openTransactionId: context.transaction!,
      };
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
    sessionJson?: string | null,
  ): OpenTransactionId {
    this.refuseOtherSession(sessionJson, "Begin transaction");
    if (this.closed) throw new Error("Begin transaction failed: the thin runtime is closed");
    if (this.transactions.has(id)) {
      throw new Error(`Begin transaction failed: transaction ${id} has already been opened`);
    }
    // The server binds the transaction to this connection's session.
    const { answer: begun, oneShot: begin } = this.requestWith({
      type: "begin",
      transaction: id,
      kind,
    });
    begun.catch(() => undefined);
    this.transactions.set(id, {
      kind,
      requests: [begun],
      staged: false,
      begin: begin!,
      lost: false,
    });
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
    if (transaction.lost) return Promise.reject(new ThinTransactionLostError(id));
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
    // The server already abandoned a lost transaction.
    if (transaction.lost) return true;
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
    if (transaction.lost) throw new ThinTransactionLostError(id);
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
    const id = await txId;
    const write = this.writes.get(id);
    if (!write) {
      // Forgotten once committed (see pruneWrites); ids are this runtime's own.
      if (typeof id === "string" && id.startsWith("thin:")) return;
      throw new Error(`unknown thin write ${String(id)}`);
    }
    // The server confirms a thin write only once it committed on the
    // authority, which meets every durability tier.
    if (!options?.observeOnly) write.observed = true;
    if (!options?.signal) return write.done;
    const signal = options.signal;
    signal.throwIfAborted();
    return Promise.race([
      write.done,
      new Promise<never>((_, reject) =>
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        }),
      ),
    ]);
  }

  /** The upstream link as the app sees it (see `linkStatusFromUpstream`). */
  upstreamLinkStatus(): LinkStatus {
    if (this.stopped) return offlineLink("stopped");
    if (this.refused) return offlineLink("auth");
    if (this.bound) return CONNECTED;
    return this.outagePublished ? offlineLink("network") : CONNECTING;
  }

  private setRefused(refused: boolean): void {
    if (this.refused === refused) return;
    this.refused = refused;
    this.notifySyncStatus();
  }

  private clearOutage(): void {
    this.failingSince = undefined;
    this.outagePublished = false;
  }

  private stop(): void {
    this.clearOutage();
    if (this.stopped) return;
    this.stopped = true;
    this.notifySyncStatus();
  }

  /** How many writes still wait for the server's answer. */
  pendingUploadCount(): number {
    return this.unanswered;
  }

  /** Observe changes of {@link pendingUploadCount} and {@link upstreamLinkStatus}. */
  onSyncStatusChange(listener: () => void, signal: AbortSignal): void {
    if (signal.aborted) return;
    this.syncStatusListeners.add(listener);
    signal.addEventListener("abort", () => this.syncStatusListeners.delete(listener), {
      once: true,
    });
  }

  private notifySyncStatus(): void {
    for (const listener of [...this.syncStatusListeners]) listener();
  }

  /** Every write made so far has been answered. Every write is global once answered. */
  async waitForPendingWrites(
    _tier?: string,
    options: { throughOutage?: boolean; signal?: AbortSignal } = {},
  ): Promise<void> {
    // Only writes still waiting for their answer, like a local-first client:
    // a rejection the app already had doesn't fail every later wait.
    const dones = [...this.writes.values()]
      .filter((write) => !write.answered)
      .map((write) => write.done);
    // Waiting for sync fails on the server's rejection. A write whose
    // connection ended unanswered is not rejected, and the shutdown drain
    // only waits for every write to be answered.
    const answered = options.throughOutage
      ? Promise.all(
          dones.map((done) =>
            done.catch((error: unknown) => {
              if (error instanceof ThinRemoteError) throw error;
            }),
          ),
        )
      : Promise.allSettled(dones);
    const signal = options.signal;
    if (!signal) {
      await answered;
      return;
    }
    signal.throwIfAborted();
    await Promise.race([
      answered,
      new Promise<never>((_, reject) =>
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        }),
      ),
    ]);
  }

  private write(op: ThinOp): TxId {
    const txId = `thin:${this.nextRequestId}` as TxId;
    const write: Write = {
      observed: false,
      committed: false,
      answered: false,
      done: Promise.resolve(),
    };
    // A standalone write goes under this runtime's next write id, so it can
    // be sent again after a lost answer. Ids follow the order writes are
    // made in, which is the order they reach the server, resent or not.
    const resendable = RESENDABLE_WRITES.has(op.type) && "options" in op;
    const sent = resendable
      ? {
          ...op,
          options: { ...op.options, writeKey: { node: this.writerNode, seq: this.nextWriteSeq++ } },
        }
      : op;
    const { answer, oneShot } = this.requestWith(sent as ThinOp);
    if (oneShot) {
      oneShot.write = true;
      oneShot.resendable = resendable;
    }
    write.done = answer.then(
      (body) => {
        if (body.type !== "written") throw new Error(`unexpected answer to a write: ${body.type}`);
      },
      (error: unknown) => {
        const failure = error instanceof Error ? error : new Error(String(error));
        // A failure no wait observes is reported once, like a local-first
        // client's rejected write.
        queueMicrotask(() => {
          if (write.observed) return;
          // Only the server's answer is a rejection. A write whose
          // connection ended unanswered may have committed: its fate is
          // unknown, not rejected.
          const rejected = failure instanceof ThinRemoteError;
          const code = rejected ? failure.code : "outcome_unknown";
          this.mutationErrorCallback?.({
            code,
            reason: failure.message,
            transaction: {
              transactionId: txId,
              kind: "mergeable",
              sealed: true,
              latestSettlement: rejected
                ? {
                    kind: "rejected",
                    transactionId: txId,
                    code,
                    reason: failure.message,
                  }
                : { kind: "missing", transactionId: txId },
            },
          });
        });
        throw failure;
      },
    );
    // An unobserved failure is reported above, not as an unhandled rejection.
    write.done.catch(() => undefined);
    this.unanswered++;
    this.notifySyncStatus();
    void write.done
      .catch(() => undefined)
      .then(() => {
        write.answered = true;
        this.unanswered--;
        this.notifySyncStatus();
      });
    write.done.then(
      () => (write.committed = true),
      () => undefined,
    );
    this.writes.set(txId, write);
    this.pruneWrites();
    return txId;
  }

  /**
   * Forget the oldest committed writes beyond {@link RETAINED_WRITES}: a
   * long session makes unboundedly many. A wait on a forgotten write
   * resolves, as every forgotten write committed.
   */
  private pruneWrites(): void {
    if (this.writes.size <= RETAINED_WRITES) return;
    for (const [txId, write] of this.writes) {
      if (this.writes.size <= RETAINED_WRITES) break;
      if (write.committed) this.writes.delete(txId);
    }
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

/** The open transaction a request belongs to, if any. */
function transactionOf(op: ThinOp): OpenTransactionId | undefined {
  const transaction = (op as { transaction?: unknown }).transaction;
  return typeof transaction === "string" ? (transaction as OpenTransactionId) : undefined;
}

/** The local refusal of a request past what one takes. */
function tooLarge(op: string, bytes: number): ThinRemoteError {
  return new ThinRemoteError(
    "TooLarge",
    `this ${op} is ${bytes} bytes, more than the ${MAX_THIN_MESSAGE_BYTES} a thin client ` +
      `sends in one request: write values that large with insertStreaming or updateStreaming`,
  );
}
