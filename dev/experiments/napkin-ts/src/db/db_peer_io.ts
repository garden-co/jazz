// Split from db.rs: auxiliary chunk routing (PeerChunkResolver, PeerIoPump).
import { blake3, toHex, deferred } from "../support/index.js";
import type {
  ChunkRequestEntry,
  ChunkResponse,
  ChunkResponseEntry,
  SyncMessage,
} from "../_extern/db-core.js";
import {
  AUXILIARY_CHANNEL,
  AuxiliaryChannelEndpoint,
  ChunkError,
  MAX_CHUNK_REQUEST_BATCH_ENTRIES,
  decodeSyncMessage,
  decodeSyncMessageTrusted,
  encodeSyncMessage,
} from "../_extern/db-core.js";

const DEFAULT_CHUNK_FORWARD_HOPS = 8;
const MAX_PENDING_CHUNK_DEMANDS = 4096;
const MAX_RELAY_CHUNK_OBLIGATIONS = MAX_PENDING_CHUNK_DEMANDS;
const MAX_CHUNK_RELAY_TRACE_EVENTS_PER_CONNECTION = 64;

export interface PeerIoTraceEntry {
  event: string;
  role: string;
  connection: number;
  requestId: number;
  remainingHops: number;
  objectHash: string;
  locatorFingerprint: string;
  response?: string;
  storageError?: string;
}

const shortFingerprint = (bytes: Uint8Array): string => toHex(blake3(bytes)).slice(0, 12);

function responseKind(response: ChunkResponse): string {
  switch (response.kind) {
    case "Found":
      return "found";
    case "Unavailable":
      return "unavailable";
    case "Retryable":
      return "retryable";
  }
  throw new Error("unreachable");
}

function storageErrorKind(error: { kind: string }): string {
  switch (error.kind) {
    case "Unavailable":
      return "unavailable";
    case "LocatorConflict":
      return "locator-conflict";
    case "Integrity":
      return "integrity";
    default:
      return "backend";
  }
}

export interface ChunkRequest {
  objectHash: Uint8Array;
  locator: any;
}
const chunkKey = (r: ChunkRequest) => toHex(r.objectHash) + ":" + toHex(r.locator.asBytes());

type ChunkResult = { ok: true; bytes: Uint8Array } | { ok: false; error: any };

type ChunkDemandWaiter =
  | { kind: "Local"; waiterId: number; sender: (r: ChunkResult) => void }
  | { kind: "Relay"; connection: number; requestId: number };

interface PendingChunkDemand {
  request: ChunkRequest;
  upstreamId: number;
  remainingHops: number;
  waiters: ChunkDemandWaiter[];
}

class ChunkDemandState {
  nextRequestId = 0;
  nextWaiterId = 0;
  relayChunkObligations = 0;
  pendingByChunk = new Map<string, PendingChunkDemand>();
  chunkByUpstreamId = new Map<number, ChunkRequest>();
  outbound: ChunkRequestEntry[] = [];
  relayResponses = new Map<number, ChunkResponseEntry[]>();
  inflightRelayResponses = new Map<number, ChunkResponseEntry[][]>();
  outboundWakers = new Map<number, () => void>();
  disconnectedConnections = new Set<number>();
  upstreamConnection: number | undefined;
  upstreamConnections = new Set<number>();
  completionGeneration = 0;
  traceByConnection = new Map<number, PeerIoTraceEntry[]>();
  tracedConnections = new Set<number>();
}

const sameBatch = (a: ChunkResponseEntry[], b: ChunkResponseEntry[]) =>
  a.length === b.length &&
  a.every((x, i) => x === b[i] || (x.requestId === b[i].requestId && x.result === b[i].result));

export class PeerChunkResolver {
  state = new ChunkDemandState();

  recordRequest(
    connection: number,
    role: PeerIoPumpRole,
    event: string,
    request: ChunkRequestEntry,
    response?: ChunkResponse,
    storageError?: { kind: string },
  ): void {
    const s = this.state;
    if (!s.tracedConnections.has(connection)) return;
    let trace = s.traceByConnection.get(connection);
    if (!trace) s.traceByConnection.set(connection, (trace = []));
    if (trace.length >= MAX_CHUNK_RELAY_TRACE_EVENTS_PER_CONNECTION) trace.shift();
    trace.push({
      event,
      role: role === "Upstream" ? "upstream" : "subscriber",
      connection,
      requestId: request.requestId,
      remainingHops: request.remainingHops,
      objectHash: shortFingerprint(request.expectedHash),
      locatorFingerprint: shortFingerprint(request.locator.asBytes()),
      response: response && responseKind(response),
      storageError: storageError && storageErrorKind(storageError),
    });
  }

  takeTrace(connection: number): PeerIoTraceEntry[] {
    const t = this.state.traceByConnection.get(connection) ?? [];
    this.state.traceByConnection.delete(connection);
    return t;
  }

  setTraceEnabled(connection: number, enabled: boolean): void {
    if (enabled) this.state.tracedConnections.add(connection);
    else {
      this.state.tracedConnections.delete(connection);
      this.state.traceByConnection.delete(connection);
    }
  }

  hasPendingLocalDemand(): boolean {
    for (const p of this.state.pendingByChunk.values())
      if (p.waiters.some((w) => w.kind === "Local")) return true;
    return false;
  }

  completionGeneration(): number {
    return this.state.completionGeneration;
  }

  registerConnection(connection: number, upstream: boolean): void {
    const s = this.state;
    s.disconnectedConnections.delete(connection);
    if (upstream) {
      s.upstreamConnections.add(connection);
      if (s.upstreamConnection === undefined) {
        s.upstreamConnection = connection;
        PeerChunkResolver.requeuePendingDemands(s);
        PeerChunkResolver.wakeConnection(s, connection);
      }
    }
  }

  static wakeConnection(s: ChunkDemandState, connection: number): void {
    const waker = s.outboundWakers.get(connection);
    if (waker) {
      s.outboundWakers.delete(connection);
      waker();
    }
  }

  static requeuePendingDemands(s: ChunkDemandState): void {
    const queued = new Set(s.outbound.map((e) => e.requestId));
    for (const pending of s.pendingByChunk.values()) {
      if (queued.has(pending.upstreamId)) continue;
      s.outbound.push({
        requestId: pending.upstreamId,
        locator: pending.request.locator,
        expectedHash: pending.request.objectHash,
        remainingHops: pending.remainingHops,
      });
    }
  }

  static reserveRelayObligation(s: ChunkDemandState): boolean {
    if (s.relayChunkObligations >= MAX_RELAY_CHUNK_OBLIGATIONS) return false;
    s.relayChunkObligations++;
    return true;
  }

  static queueRelayResponse(
    s: ChunkDemandState,
    connection: number,
    response: ChunkResponseEntry,
    reservationTransferred: boolean,
  ): boolean {
    if (!reservationTransferred && !PeerChunkResolver.reserveRelayObligation(s)) return false;
    let q = s.relayResponses.get(connection);
    if (!q) s.relayResponses.set(connection, (q = []));
    q.push(response);
    PeerChunkResolver.wakeConnection(s, connection);
    return true;
  }

  enqueueRelayResponses(connection: number, responses: ChunkResponseEntry[]): void {
    for (const r of responses)
      if (!PeerChunkResolver.queueRelayResponse(this.state, connection, r, false)) break;
  }

  enqueue(
    request: ChunkRequest,
    remainingHops: number,
    waiter: ChunkDemandWaiter,
  ): ChunkDemandWaiter | undefined {
    const s = this.state;
    const relayWaiter = waiter.kind === "Relay";
    if (relayWaiter && !PeerChunkResolver.reserveRelayObligation(s)) return waiter;
    const key = chunkKey(request);
    const pending = s.pendingByChunk.get(key);
    if (pending) {
      pending.waiters.push(waiter);
      return undefined;
    }
    if (s.pendingByChunk.size >= MAX_PENDING_CHUNK_DEMANDS) {
      if (relayWaiter) s.relayChunkObligations--;
      return waiter;
    }
    s.nextRequestId = Math.max((s.nextRequestId + 1) >>> 0, 1);
    const upstreamId = s.nextRequestId;
    s.outbound.push({
      requestId: upstreamId,
      locator: request.locator,
      expectedHash: request.objectHash,
      remainingHops,
    });
    s.chunkByUpstreamId.set(upstreamId, request);
    s.pendingByChunk.set(key, { request, upstreamId, remainingHops, waiters: [waiter] });
    if (s.upstreamConnection !== undefined)
      PeerChunkResolver.wakeConnection(s, s.upstreamConnection);
    return undefined;
  }

  enqueueRelay(connection: number, request: ChunkRequestEntry): void {
    if (request.remainingHops === 0) {
      PeerChunkResolver.queueRelayResponse(
        this.state,
        connection,
        { requestId: request.requestId, result: { kind: "Unavailable" } },
        false,
      );
      return;
    }
    const rejected = this.enqueue(
      { objectHash: request.expectedHash, locator: request.locator },
      request.remainingHops - 1,
      { kind: "Relay", connection, requestId: request.requestId },
    );
    if (rejected && rejected.kind === "Relay") {
      PeerChunkResolver.queueRelayResponse(
        this.state,
        rejected.connection,
        { requestId: rejected.requestId, result: { kind: "Retryable", retryAfterMs: 25 } },
        false,
      );
    }
  }

  takeOutbound(limit: number): ChunkRequestEntry[] {
    const count = Math.min(limit, MAX_CHUNK_REQUEST_BATCH_ENTRIES, this.state.outbound.length);
    return this.state.outbound.splice(0, count);
  }

  takeRelayResponses(connection: number, limit: number): ChunkResponseEntry[] {
    const s = this.state;
    const queued = s.relayResponses.get(connection);
    if (!queued) return [];
    const responses = queued.splice(0, Math.min(limit, queued.length));
    if (!queued.length) s.relayResponses.delete(connection);
    let inflight = s.inflightRelayResponses.get(connection);
    if (!inflight) s.inflightRelayResponses.set(connection, (inflight = []));
    inflight.push(responses.slice());
    return responses;
  }

  restoreRelayResponses(connection: number, responses: ChunkResponseEntry[]): void {
    const s = this.state;
    const inflight = s.inflightRelayResponses.get(connection);
    if (!inflight) throw new Error("restored relay response batch was handed out");
    const position = inflight.findIndex((b) => sameBatch(b, responses));
    if (position < 0) throw new Error("restored relay response batch is still in flight");
    inflight.splice(position, 1);
    if (!inflight.length) s.inflightRelayResponses.delete(connection);
    const q = s.relayResponses.get(connection) ?? [];
    s.relayResponses.set(connection, [...responses, ...q]);
  }

  acknowledgeRelayResponseSend(connection: number, responses: ChunkResponseEntry[]): void {
    const s = this.state;
    const inflight = s.inflightRelayResponses.get(connection);
    if (!inflight) throw new Error("acknowledged relay response batch was handed out");
    const position = inflight.findIndex((b) => sameBatch(b, responses));
    if (position < 0) throw new Error("acknowledged relay response batch is still in flight");
    const [released] = inflight.splice(position, 1);
    if (!inflight.length) s.inflightRelayResponses.delete(connection);
    if (s.relayChunkObligations < released.length)
      throw new Error("acknowledged relay responses are accounted");
    s.relayChunkObligations -= released.length;
  }

  isActiveUpstream(connection: number): boolean {
    return this.state.upstreamConnection === connection;
  }

  complete(response: ChunkResponseEntry): void {
    const s = this.state;
    const request = s.chunkByUpstreamId.get(response.requestId);
    if (!request) return;
    s.chunkByUpstreamId.delete(response.requestId);
    const key = chunkKey(request);
    const pending = s.pendingByChunk.get(key);
    if (!pending) return;
    s.pendingByChunk.delete(key);
    s.completionGeneration = (s.completionGeneration + 1) >>> 0;
    for (const waiter of pending.waiters) {
      if (waiter.kind === "Local") {
        const r = response.result;
        waiter.sender(
          r.kind === "Found"
            ? { ok: true, bytes: r.bytes.slice() }
            : r.kind === "Unavailable"
              ? { ok: false, error: ChunkError.Unavailable }
              : { ok: false, error: ChunkError.retryable(r.retryAfterMs) },
        );
      } else {
        PeerChunkResolver.queueRelayResponse(
          s,
          waiter.connection,
          { requestId: waiter.requestId, result: response.result },
          true,
        );
      }
    }
  }

  private dropPending(key: string, pending: PendingChunkDemand): void {
    const s = this.state;
    s.pendingByChunk.delete(key);
    s.chunkByUpstreamId.delete(pending.upstreamId);
    s.outbound = s.outbound.filter((o) => o.requestId !== pending.upstreamId);
  }

  failLocalDemandForClose(): void {
    const s = this.state;
    let failed = false;
    for (const [key, pending] of [...s.pendingByChunk]) {
      const waiters = pending.waiters;
      pending.waiters = [];
      for (const waiter of waiters) {
        if (waiter.kind === "Local") {
          failed = true;
          waiter.sender({ ok: false, error: ChunkError.Unavailable });
        } else pending.waiters.push(waiter);
      }
      if (!pending.waiters.length) this.dropPending(key, pending);
    }
    if (failed) s.completionGeneration = (s.completionGeneration + 1) >>> 0;
  }

  cancelLocal(request: ChunkRequest, waiterId: number): void {
    const key = chunkKey(request);
    const pending = this.state.pendingByChunk.get(key);
    if (!pending) return;
    pending.waiters = pending.waiters.filter(
      (w) => !(w.kind === "Local" && w.waiterId === waiterId),
    );
    if (!pending.waiters.length) this.dropPending(key, pending);
  }

  disconnect(connection: number, upstream: boolean): void {
    const s = this.state;
    s.disconnectedConnections.add(connection);
    s.traceByConnection.delete(connection);
    s.tracedConnections.delete(connection);
    const disconnectedWaker = s.outboundWakers.get(connection);
    s.outboundWakers.delete(connection);
    const queued = s.relayResponses.get(connection)?.length ?? 0;
    s.relayResponses.delete(connection);
    const inflight = (s.inflightRelayResponses.get(connection) ?? []).reduce(
      (n, b) => n + b.length,
      0,
    );
    s.inflightRelayResponses.delete(connection);
    if (s.relayChunkObligations < queued + inflight)
      throw new Error("disconnected relay responses are accounted");
    s.relayChunkObligations -= queued + inflight;
    if (upstream) {
      s.upstreamConnections.delete(connection);
      if (s.upstreamConnection === connection) {
        s.upstreamConnection = [...s.upstreamConnections].sort((a, b) => a - b)[0];
        if (s.upstreamConnection !== undefined) {
          PeerChunkResolver.requeuePendingDemands(s);
          PeerChunkResolver.wakeConnection(s, s.upstreamConnection);
        }
      }
      disconnectedWaker?.();
      return;
    }
    for (const [key, pending] of [...s.pendingByChunk]) {
      const before = pending.waiters.length;
      pending.waiters = pending.waiters.filter(
        (w) => !(w.kind === "Relay" && w.connection === connection),
      );
      const removed = before - pending.waiters.length;
      if (s.relayChunkObligations < removed)
        throw new Error("disconnected relay waiters are accounted");
      s.relayChunkObligations -= removed;
      if (!pending.waiters.length) this.dropPending(key, pending);
    }
    disconnectedWaker?.();
  }

  /** groove::chunks::MissingChunkResolver */
  resolve(request: ChunkRequest): Promise<Uint8Array> & { cancel(): void } {
    const d = deferred<ChunkResult>();
    const s = this.state;
    s.nextWaiterId = Math.max((s.nextWaiterId + 1) >>> 0, 1);
    const waiterId = s.nextWaiterId;
    const rejected = this.enqueue(request, DEFAULT_CHUNK_FORWARD_HOPS, {
      kind: "Local",
      waiterId,
      sender: d.resolve,
    });
    if (rejected && rejected.kind === "Local")
      rejected.sender({ ok: false, error: ChunkError.backend("chunk request backpressure") });
    let completed = false;
    const p = d.promise.then((r) => {
      completed = true;
      if (r.ok) return r.bytes;
      throw r.error;
    }) as Promise<Uint8Array> & { cancel(): void };
    p.cancel = () => {
      if (!completed) this.cancelLocal(request, waiterId);
    };
    return p;
  }
}

export type PeerIoPumpRole = "Upstream" | "Subscriber";

export class ReservedOutboundWireFrame {
  constructor(
    readonly pump: PeerIoPump,
    readonly frame: Uint8Array,
    readonly creditGrant: boolean,
  ) {}

  takeFrame(): Uint8Array {
    return this.frame.slice();
  }

  commit(): void {
    this.pump.wireReservationActive.v = false;
    if (this.creditGrant) {
      this.pump.channelCredits().acceptGrant();
      return;
    }
    const endpoint = this.pump.auxiliaryEndpoint;
    if (!endpoint) throw new Error("reserved frame has an auxiliary endpoint");
    const completed = endpoint.acceptOutbound();
    if (completed !== undefined) {
      const message = this.pump.wireOutboundObligation.v;
      this.pump.wireOutboundObligation.v = undefined;
      if (message) this.pump.acknowledgeOutbound(message);
    }
  }

  drop(): void {
    this.pump.wireReservationActive.v = false;
  }
}

export class PeerIoPump {
  auxiliaryEndpoint: any;
  wireReservationActive = { v: false };
  wireOutboundObligation: { v: SyncMessage | undefined } = { v: undefined };
  canonicalCreditProgress = { v: false };

  constructor(
    readonly resolver: PeerChunkResolver,
    readonly localChunks: any,
    readonly connection: number,
    readonly role: PeerIoPumpRole,
    readonly wireInboundContextValue: any,
  ) {
    resolver.registerConnection(connection, role === "Upstream");
    if (wireInboundContextValue) {
      const endpoint = AuxiliaryChannelEndpoint.new(wireInboundContextValue);
      endpoint.setPumpOwned();
      this.auxiliaryEndpoint = endpoint;
    }
  }

  withSharedAuxiliaryEndpoint(endpoint: any): this {
    if (endpoint) this.auxiliaryEndpoint = endpoint;
    return this;
  }

  claimBindingOutput(): void {
    this.auxiliaryEndpoint?.setPumpOwned();
  }

  incompleteReceiveTimeoutMs(): number | undefined {
    return this.auxiliaryEndpoint?.incompleteReceiveTimeoutMs();
  }

  expireIncompleteReceive(): void {
    if (!this.auxiliaryEndpoint) throw new Error("auxiliary endpoint unavailable");
    this.auxiliaryEndpoint.expireIncompleteReceive();
  }

  wireInboundContext(): any {
    if (!this.wireInboundContextValue)
      throw new Error("auxiliary wire framing requires a paired wire transport adapter");
    return this.wireInboundContextValue;
  }

  /** Returns the message back for canonical traffic the binding must enqueue. */
  async routeIncoming(message: SyncMessage): Promise<SyncMessage | undefined> {
    if (this.role === "Upstream" && message.kind === "ChunkResponseBatch") {
      if (!this.resolver.isActiveUpstream(this.connection)) return undefined;
      for (const response of message.batch.responses) {
        const s = this.resolver.state;
        const request = s.chunkByUpstreamId.get(response.requestId);
        const pending = request && s.pendingByChunk.get(chunkKey(request));
        if (request && pending) {
          this.resolver.recordRequest(
            this.connection,
            this.role,
            "inbound-response",
            {
              requestId: response.requestId,
              locator: request.locator,
              expectedHash: request.objectHash,
              remainingHops: pending.remainingHops,
            },
            response.result,
          );
        }
        this.resolver.complete(response);
      }
      return undefined;
    }
    if (this.role === "Subscriber" && message.kind === "ChunkRequestBatch") {
      const responses: ChunkResponseEntry[] = [];
      for (const request of message.batch.requests) {
        if (this.isDisconnected()) break;
        this.resolver.recordRequest(this.connection, this.role, "inbound-request", request);
        let result: { ok: true; bytes: Uint8Array } | { ok: false; error: any };
        try {
          result = {
            ok: true,
            bytes: await this.localChunks.get(request.locator, request.expectedHash),
          };
        } catch (error) {
          result = { ok: false, error };
        }
        if (this.isDisconnected()) break;
        if (result.ok) {
          this.resolver.recordRequest(this.connection, this.role, "local-response", request, {
            kind: "Found",
            bytes: new Uint8Array(),
          });
          responses.push({
            requestId: request.requestId,
            result: { kind: "Found", bytes: result.bytes },
          });
        } else if (result.error?.kind === "Unavailable") {
          this.resolver.recordRequest(this.connection, this.role, "relay-request", request);
          this.resolver.enqueueRelay(this.connection, request);
        } else {
          this.resolver.recordRequest(
            this.connection,
            this.role,
            "local-response",
            request,
            { kind: "Unavailable" },
            result.error,
          );
          responses.push({ requestId: request.requestId, result: { kind: "Unavailable" } });
        }
      }
      if (responses.length && !this.isDisconnected())
        this.resolver.enqueueRelayResponses(this.connection, responses);
      return undefined;
    }
    if (message.kind === "ChunkRequestBatch" || message.kind === "ChunkResponseBatch")
      return undefined;
    return message;
  }

  async routeIncomingPayload(payload: Uint8Array): Promise<Uint8Array | undefined> {
    let message: SyncMessage;
    try {
      message =
        this.role === "Upstream" ? decodeSyncMessageTrusted(payload) : decodeSyncMessage(payload);
    } catch (error) {
      throw new Error(`malformed auxiliary chunk payload: ${error}`);
    }
    return (await this.routeIncoming(message)) === undefined ? undefined : payload;
  }

  channelCredits(): any {
    if (!this.auxiliaryEndpoint) throw new Error("auxiliary channel endpoint is unavailable");
    return this.auxiliaryEndpoint.channelCredits();
  }

  takeCanonicalCreditProgress(): boolean {
    const v = this.canonicalCreditProgress.v;
    this.canonicalCreditProgress.v = false;
    return v;
  }

  async routeIncomingWireFrame(frame: Uint8Array): Promise<Uint8Array | undefined> {
    if (this.isDisconnected()) throw new Error("auxiliary connection is disconnected");
    const context = this.wireInboundContext();
    let decoded: any;
    try {
      decoded = context.decodeFrame(frame);
    } catch (error) {
      throw new Error(`malformed auxiliary wire frame: ${error}`);
    }
    if (decoded.kind === "ChannelCredit") {
      this.channelCredits().receiveCredit(decoded.grant);
      this.canonicalCreditProgress.v = true;
      return undefined;
    }
    if (decoded.kind !== "Channel") return frame;
    const envelope = decoded.envelope;
    if (envelope.extent.channel !== AUXILIARY_CHANNEL) return frame;
    if (!this.auxiliaryEndpoint) throw new Error("auxiliary channel endpoint is unavailable");
    const message = this.auxiliaryEndpoint.receive(envelope, frame.length);
    if (message !== undefined && (await this.routeIncoming(message)) !== undefined)
      throw new Error("canonical message on reserved auxiliary channel");
    return undefined;
  }

  takeOutboundWireFrame(): Uint8Array | undefined {
    const reservation = this.reserveOutboundWireFrame();
    if (!reservation) return undefined;
    const frame = reservation.takeFrame();
    reservation.commit();
    return frame;
  }

  reserveOutboundWireFrame(): ReservedOutboundWireFrame | undefined {
    if (this.isDisconnected() || this.wireReservationActive.v) return undefined;
    const grant = this.channelCredits().peekGrant();
    if (grant !== undefined) {
      this.wireReservationActive.v = true;
      return new ReservedOutboundWireFrame(this, grant, true);
    }
    const endpoint = this.auxiliaryEndpoint;
    if (!endpoint)
      throw new Error("auxiliary wire framing requires a paired wire transport adapter");
    if (!endpoint.hasPendingOutbound()) {
      const message = this.takeOutbound(1);
      if (!message) return undefined;
      try {
        endpoint.enqueue(message);
      } catch (error) {
        this.restoreOutbound(message);
        throw new Error(`cannot enqueue auxiliary channel message: ${error}`);
      }
      this.wireOutboundObligation.v = message;
    }
    const frame = endpoint.peekOutbound();
    if (frame === undefined) return undefined;
    this.wireReservationActive.v = true;
    return new ReservedOutboundWireFrame(this, frame, false);
  }

  takeOutboundWireFrames(maxFrames: number, maxBytes: number): Uint8Array[] {
    const frames: Uint8Array[] = [];
    let total = 0;
    while (frames.length < maxFrames && total < maxBytes) {
      const reservation = this.reserveOutboundWireFrame();
      if (!reservation) break;
      if (reservation.frame.length > maxBytes - total) {
        reservation.drop();
        if (!frames.length)
          throw new Error(
            `auxiliary wire frame exceeds bounded drain budget: frame=${reservation.frame.length} budget=${maxBytes}`,
          );
        break;
      }
      const frame = reservation.takeFrame();
      total += frame.length;
      reservation.commit();
      frames.push(frame);
    }
    return frames;
  }

  takeOutbound(limit: number): SyncMessage | undefined {
    if (this.role === "Upstream") {
      const requests = this.resolver.takeOutbound(limit);
      for (const r of requests)
        this.resolver.recordRequest(this.connection, this.role, "outbound-request", r);
      return requests.length ? { kind: "ChunkRequestBatch", batch: { requests } } : undefined;
    }
    const responses = this.resolver.takeRelayResponses(this.connection, limit);
    return responses.length ? { kind: "ChunkResponseBatch", batch: { responses } } : undefined;
  }

  sendOutbound(transport: { send(m: SyncMessage): void }, limit: number): boolean {
    const message = this.takeOutbound(limit);
    if (!message) return false;
    try {
      transport.send(message);
    } catch (error) {
      this.restoreOutbound(message);
      throw error;
    }
    this.acknowledgeOutbound(message);
    return true;
  }

  restoreOutbound(message: SyncMessage): void {
    if (this.role === "Upstream" && message.kind === "ChunkRequestBatch")
      this.resolver.state.outbound.unshift(...message.batch.requests);
    else if (this.role === "Subscriber" && message.kind === "ChunkResponseBatch")
      this.resolver.restoreRelayResponses(this.connection, message.batch.responses);
  }

  acknowledgeOutbound(message: SyncMessage): void {
    if (this.role === "Subscriber" && message.kind === "ChunkResponseBatch")
      this.resolver.acknowledgeRelayResponseSend(this.connection, message.batch.responses);
  }

  takeOutboundPayload(limit: number): Uint8Array | undefined {
    const message = this.takeOutbound(limit);
    if (!message) return undefined;
    try {
      const payload = encodeSyncMessage(message);
      this.acknowledgeOutbound(message);
      return payload;
    } catch (error) {
      this.restoreOutbound(message);
      throw new Error(`cannot encode auxiliary chunk payload: ${error}`);
    }
  }

  outboundReady(): Promise<void> {
    return new Promise<void>((resolve) => {
      const poll = () => {
        try {
          if (this.channelCredits().pollGrantReady(poll)) return resolve();
        } catch {}
        if (this.auxiliaryEndpoint?.pollOutboundReady(poll)) return resolve();
        if (this.hasOutbound() || this.isDisconnected()) return resolve();
        this.resolver.state.outboundWakers.set(this.connection, poll);
      };
      poll();
    });
  }

  outboundIsReady(): boolean {
    return this.hasOutbound();
  }

  disconnect(): void {
    try {
      this.auxiliaryEndpoint?.channelCredits().close();
    } catch {}
    this.resolver.disconnect(this.connection, this.role === "Upstream");
  }

  hasOutbound(): boolean {
    if (this.isDisconnected()) return false;
    const endpoint = this.auxiliaryEndpoint;
    if (endpoint) {
      const credits = endpoint.channelCredits();
      if (credits.hasPendingGrant()) return true;
      if (!credits.canSend("Auxiliary")) return false;
      if (endpoint.outboundIsReady()) return true;
    }
    const s = this.resolver.state;
    return this.role === "Upstream"
      ? s.outbound.length > 0
      : (s.relayResponses.get(this.connection)?.length ?? 0) > 0;
  }

  isDisconnected(): boolean {
    return this.resolver.state.disconnectedConnections.has(this.connection);
  }

  takeTrace(): PeerIoTraceEntry[] {
    return this.resolver.takeTrace(this.connection);
  }

  setTraceEnabled(enabled: boolean): void {
    this.resolver.setTraceEnabled(this.connection, enabled);
  }
}
