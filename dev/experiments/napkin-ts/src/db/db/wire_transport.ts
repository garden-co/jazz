// Wire-frame adaptation, bounded-message fragmentation, and reassembly.
import { encode } from "../../support/postcard.js";
import { concatBytes, toHex } from "../../support/bytes.js";
import {
  AUXILIARY_CHANNEL,
  ChannelClass,
  type ConnectionSessionContext,
  MAX_CHANNELS,
  PROGRESS_CHANNEL,
  type SyncMessage,
  TransportError,
  type ViewUpdatePayload,
  WIRE_PROTOCOL_VERSION,
  WireError,
  WireInboundContext,
  type WireSession,
  type WireTransport,
  currentWireFeatures,
  encodeFrame,
  encodeSubscriptionKey,
  encodeViewUpdatePayload,
  ensureSyncMessageFeatures,
  validateWireFrameLen,
} from "../../_extern/db-rest-wasm.js";
import { AuxiliaryChannelEndpoint, ChannelEndpoint, messageClass } from "./channel_endpoint.js";
import {
  type ReceivedSyncMessage,
  maxRoutedPayloadBytes,
  unleasedMessage,
} from "./routed_messages.js";
import { mergeViewUpdateParts, splitViewUpdate } from "./view_update_parts.js";

export type WireFlushStatus = "Idle" | "MoreReady" | "Backpressured";
export type WireSendOutcome = { kind: "Accepted" } | { kind: "Rejected"; message: SyncMessage };

interface InboundViewParts {
  parts: ViewUpdatePayload[];
  receiptsValidated: boolean;
}

const isBackpressure = (e: unknown) => (e as any)?.kind === "Backpressure";
const failed = (e: unknown) => new TransportError("Failed", String(e));

/** Converts logical messages to mandatory wire-v3 ordered channels. */
export class WireTransportAdapter {
  inboundContext: any;
  endpoint: ChannelEndpoint;
  auxiliary: AuxiliaryChannelEndpoint;
  routes = new Map<string, [number, number]>();
  canonicalTurns = 0;
  terminalError: Error | undefined;
  lastWireError: WireError | undefined;
  receivedWireError = false;
  outboundViewParts: SyncMessage[] = [];
  inboundViewParts = new Map<string, InboundViewParts>();

  static current(inner: WireTransport): WireTransportAdapter {
    return new WireTransportAdapter(inner, WIRE_PROTOCOL_VERSION, currentWireFeatures());
  }

  constructor(
    public inner: WireTransport,
    protocolVersion: number,
    features: any,
    session?: WireSession,
    public sessionContext?: ConnectionSessionContext,
    public permitsDelegatedSessions = false,
  ) {
    const context = new WireInboundContext(protocolVersion, features, session);
    this.inboundContext = context;
    this.endpoint = new ChannelEndpoint(context.clone());
    this.auxiliary = new AuxiliaryChannelEndpoint(context.clone());
    this.auxiliary.setChannelCredits(this.endpoint.channelCredits());
  }

  private route(message: SyncMessage): [number, number, any, boolean] {
    const [klass, barrier] = messageClass(message);
    const fixed =
      klass === ChannelClass.Control
        ? 0
        : klass === ChannelClass.Requests
          ? 1
          : klass === ChannelClass.Writes
            ? 2
            : klass === ChannelClass.Progress
              ? PROGRESS_CHANNEL
              : undefined;
    if (fixed !== undefined) return [fixed, 0, klass, barrier];
    const hit = this.routes.get(deliveryRouteKey(message));
    if (hit) return [hit[0], hit[1], klass, barrier];
    let slot = -1;
    for (let s = 3; s < PROGRESS_CHANNEL; s++)
      if (this.endpoint.isIdle(s)) {
        slot = s;
        break;
      }
    if (slot < 0) throw new TransportError("Backpressure");
    let generation: number;
    try {
      generation = this.endpoint.nextIdleGeneration(slot);
    } catch (e) {
      throw failed(e);
    }
    return [slot, generation, klass, barrier];
  }

  private flushTurn(turns: number): WireFlushStatus {
    if (this.terminalError) throw this.terminalError;
    this.enqueueOutboundViewParts();
    const status = this.flushEndpoints(turns);
    if (!this.outboundViewParts.length) return status;
    this.enqueueOutboundViewParts();
    if (status === "Idle" && this.endpoint.hasPending()) return "MoreReady";
    if (status === "Idle" && this.outboundViewParts.length) return "Backpressured";
    return status;
  }

  /** `false` is backpressure: nothing was admitted. */
  private enqueueCanonical(message: SyncMessage): boolean {
    let route: [number, number, any, boolean];
    try {
      route = this.route(message);
    } catch (e) {
      if (isBackpressure(e)) return false;
      throw e;
    }
    const [slot, generation, klass, barrier] = route;
    try {
      this.endpoint.enqueue(slot, generation, klass, message, barrier);
    } catch (e) {
      if (isBackpressure(e)) return false;
      throw e;
    }
    if (slot >= 3 && slot < PROGRESS_CHANNEL) {
      for (const [k, [existing]] of this.routes) if (existing === slot) this.routes.delete(k);
      this.routes.set(deliveryRouteKey(message), [slot, generation]);
    }
    return true;
  }

  private enqueueOutboundViewParts(): void {
    while (this.outboundViewParts.length) {
      const part = this.outboundViewParts[0];
      try {
        if (!this.enqueueCanonical(part)) break;
      } catch (error) {
        this.outboundViewParts = [];
        this.terminalError = error as Error;
        throw error;
      }
      this.outboundViewParts.shift();
    }
  }

  private assembleViewUpdate(received: ReceivedSyncMessage): ReceivedSyncMessage | undefined {
    const { message, lease, receiptsValidated } = received;
    if (message.kind === "ViewUpdatePart") {
      const part: ViewUpdatePayload = message.payload;
      // The part's lease is released here; holding its credit could stall the sequence.
      lease?.release?.();
      const open = this.inboundViewParts.size;
      const key = toHex(encode(part.subscription, encodeSubscriptionKey));
      let entry = this.inboundViewParts.get(key);
      if (!entry) this.inboundViewParts.set(key, (entry = { parts: [], receiptsValidated: true }));
      if (!entry.parts.length && open >= MAX_CHANNELS)
        throw this.failViewUpdateAssembly(
          "more open view-update part sequences than delivery streams",
        );
      entry.parts.push(part);
      entry.receiptsValidated &&= receiptsValidated;
      return undefined;
    }
    if (message.kind === "ViewUpdate") {
      const last: ViewUpdatePayload = message.payload;
      const key = toHex(encode(last.subscription, encodeSubscriptionKey));
      const buffered = this.inboundViewParts.get(key);
      if (!buffered) return received;
      this.inboundViewParts.delete(key);
      let merged: ViewUpdatePayload;
      try {
        merged = mergeViewUpdateParts(buffered.parts, last);
      } catch (reason) {
        throw this.failViewUpdateAssembly(String(reason));
      }
      return {
        message: { kind: "ViewUpdate", payload: merged },
        lease,
        receiptsValidated: receiptsValidated && buffered.receiptsValidated,
      };
    }
    return received;
  }

  private failViewUpdateAssembly(reason: string): Error {
    this.inboundViewParts.clear();
    this.lastWireError = new WireError("MalformedFrame", "Never", reason);
    return new TransportError("Failed", reason);
  }

  /** Sends one frame; returns false on backpressure, rethrows terminal errors. */
  private sendFrame(frame: Uint8Array): boolean {
    try {
      this.inner.sendFrame(frame);
      return true;
    } catch (error) {
      if (isBackpressure(error)) return false;
      this.terminalError = error as Error;
      throw error;
    }
  }

  private flushEndpoints(turns: number): WireFlushStatus {
    try {
      this.endpoint.expire();
      this.auxiliary.expireIncompleteReceive();
    } catch (e) {
      throw failed(e);
    }
    const aux = this.auxiliary;
    for (let turn = 0; turn < turns; turn++) {
      const pumpOwned = aux.pumpOwned;
      if (!pumpOwned) {
        const credits = this.endpoint.channelCredits();
        const grant = credits.peekGrant();
        if (grant) {
          if (!this.sendFrame(grant)) return "Backpressured";
          credits.acceptGrant();
          continue;
        }
      }
      if (!pumpOwned && this.canonicalTurns >= 4 && aux.outboundIsReady()) {
        const frame = aux.peekOutbound();
        if (frame) {
          if (!this.sendFrame(frame)) return "Backpressured";
          aux.acceptOutbound();
          this.canonicalTurns = 0;
          continue;
        }
      }
      const canonical = this.endpoint.peekOutbound();
      if (canonical) {
        if (!this.sendFrame(canonical)) return "Backpressured";
        this.endpoint.acceptOutbound();
        this.canonicalTurns = Math.min(255, this.canonicalTurns + 1);
      } else {
        if (aux.pumpOwned) return this.endpoint.hasPending() ? "Backpressured" : "Idle";
        const frame = aux.peekOutbound();
        if (!frame)
          return this.endpoint.hasPending() || aux.hasPendingOutbound() ? "Backpressured" : "Idle";
        if (!this.sendFrame(frame)) return "Backpressured";
        aux.acceptOutbound();
        this.canonicalTurns = 0;
      }
    }
    return this.endpoint.hasPending() || (!aux.pumpOwned && aux.hasPendingOutbound())
      ? "MoreReady"
      : "Idle";
  }

  /** Strict receive for bootstrap and live channels. */
  tryRecvStrict(): SyncMessage | undefined {
    try {
      return this.tryRecvResult();
    } catch (error) {
      throw this.lastWireError ?? new WireError("Internal", "Never", String(error));
    }
  }

  // Terminal diagnostics are best effort.
  private sendWireError(error: WireError): void {
    try {
      this.inner.sendFrame(encodeFrame({ kind: "Error", error }));
    } catch {}
  }

  private receive(): ReceivedSyncMessage | undefined {
    for (let received; (received = this.receiveRouted()); ) {
      const message = this.assembleViewUpdate(received);
      if (message) return message;
    }
    return undefined;
  }

  private receiveRouted(): ReceivedSyncMessage | undefined {
    if (this.terminalError) throw this.terminalError;
    // Outbound backpressure does not block independent inbound progress.
    this.flushTurn(1);
    const popped = this.endpoint.pop();
    if (popped) return popped;
    for (let bytes; (bytes = this.inner.tryRecvFrame()); ) {
      let frame: any;
      try {
        validateWireFrameLen(bytes.length);
        frame = this.inboundContext.decodeFrame(bytes);
      } catch (e) {
        throw failed(e);
      }
      if (frame.kind === "Channel") {
        try {
          this.inboundContext.validateChannelMetadata(frame.envelope);
        } catch (error) {
          this.lastWireError = error as WireError;
          throw new TransportError("Failed", (error as WireError).message);
        }
      }
      let message: ReceivedSyncMessage | undefined;
      switch (frame.kind) {
        case "Channel":
          try {
            if (frame.envelope.extent.channel === AUXILIARY_CHANNEL) {
              const m = this.auxiliary.receive(frame.envelope, bytes.length);
              message = m && unleasedMessage(m);
            } else message = this.endpoint.receive(frame.envelope, bytes.length);
          } catch (e) {
            throw failed(e);
          }
          break;
        case "ChannelCredit":
          try {
            this.endpoint.channelCredits().receiveCredit(frame.grant);
          } catch (e) {
            throw failed(e);
          }
          break;
        case "Error":
          this.receivedWireError = true;
          this.lastWireError = frame.error;
          throw new TransportError("Failed", `remote wire error: ${String(frame.error)}`);
        default:
          throw new TransportError(
            "Failed",
            "live wire-v3 transport requires ordered channel frames",
          );
      }
      if (message) {
        this.flushTurn(1);
        return message;
      }
    }
    this.flushTurn(1);
    return undefined;
  }

  /** Only the peer's own retry guidance belongs here. */
  remoteWireError(): WireError | undefined {
    return this.receivedWireError ? this.lastWireError : undefined;
  }

  /** Offer one logical message, handing it back if it was not admitted. */
  offer(message: SyncMessage): WireSendOutcome {
    if (this.terminalError) throw this.terminalError;
    try {
      ensureSyncMessageFeatures(message, this.inboundContext.negotiatedFeatures());
    } catch (error) {
      this.sendWireError(error as WireError);
      return { kind: "Accepted" };
    }
    if (messageClass(message)[0] === ChannelClass.Auxiliary) {
      const rejected = this.auxiliary.tryEnqueue(message);
      if (rejected) {
        if (isBackpressure(rejected[0])) return { kind: "Rejected", message: rejected[1] };
        throw rejected[0];
      }
    } else {
      // The parts of an accepted oversized update go out contiguously.
      this.enqueueOutboundViewParts();
      if (this.outboundViewParts.length) return { kind: "Rejected", message };
      if (message.kind === "ViewUpdate" && exceedsRoutedPayloadLimit(message.payload)) {
        try {
          this.outboundViewParts = splitViewUpdate(message.payload, maxRoutedPayloadBytes());
        } catch (error) {
          throw failed((error as Error).message);
        }
        this.enqueueOutboundViewParts();
      } else if (!this.enqueueCanonical(message)) return { kind: "Rejected", message };
    }
    try {
      this.flushTurn(1);
    } catch (error) {
      this.terminalError = error as Error;
      throw error;
    }
    return { kind: "Accepted" };
  }

  send(message: SyncMessage): void {
    if (this.offer(message).kind === "Rejected") throw new TransportError("Backpressure");
  }
  trySend(message: SyncMessage): SyncMessage | undefined {
    const outcome = this.offer(message);
    return outcome.kind === "Rejected" ? outcome.message : undefined;
  }
  tryRecv(): SyncMessage | undefined {
    try {
      return this.tryRecvResult();
    } catch {
      return undefined;
    }
  }
  tryRecvResult(): SyncMessage | undefined {
    return this.tryRecvOwnedResult()?.message;
  }
  tryRecvOwnedResult(): ReceivedSyncMessage | undefined {
    const wasTerminal = this.terminalError !== undefined;
    try {
      return this.receive();
    } catch (error) {
      if (!wasTerminal) {
        const wireError =
          this.lastWireError ??
          this.endpoint.lastWireError ??
          this.auxiliary.lastWireError() ??
          new WireError("MalformedFrame", "Never", String(error));
        if (!this.receivedWireError) this.sendWireError(wireError);
        this.lastWireError = wireError;
      }
      this.terminalError = error as Error;
      throw error;
    }
  }
  pollFlush(): WireFlushStatus {
    try {
      return this.flushTurn(8);
    } catch (error) {
      this.terminalError = error as Error;
      throw error;
    }
  }
  hasTerminalFailure(): boolean {
    return this.terminalError !== undefined;
  }
  incompleteReceiveTimeoutMs(): number | undefined {
    const all = [
      this.endpoint.incompleteReceiveTimeoutMs(),
      this.auxiliary.incompleteReceiveTimeoutMs(),
    ].filter((v): v is number => v !== undefined);
    return all.length ? Math.min(...all) : undefined;
  }
  sharedAuxiliaryEndpoint(): AuxiliaryChannelEndpoint {
    return this.auxiliary;
  }
  setTrustedEncoder(trusted: boolean): void {
    this.inboundContext.setTrustedEncoder(trusted);
    this.endpoint.setTrustedEncoder(trusted);
    this.auxiliary.setTrustedEncoder(trusted);
  }
  wireInboundContext(): any {
    return this.inboundContext.clone();
  }
  connectionSessionContext(): ConnectionSessionContext | undefined {
    return this.sessionContext;
  }
}

function exceedsRoutedPayloadLimit(view: ViewUpdatePayload): boolean {
  // Measure the payload as a message: the variant tag is one byte.
  try {
    return encode(view, encodeViewUpdatePayload).length + 1 > maxRoutedPayloadBytes();
  } catch {
    return true;
  }
}

function deliveryRouteKey(message: SyncMessage): string {
  let tag = 4;
  let bytes = new Uint8Array(0);
  switch (message.kind) {
    // Every part of one update shares its subscription's stream.
    case "ViewUpdate":
    case "ViewUpdatePart":
      [tag, bytes] = [0, encode(message.payload.subscription, encodeSubscriptionKey)];
      break;
    case "SubscribeRejected":
      [tag, bytes] = [0, encode(message.subscription, encodeSubscriptionKey)];
      break;
    case "AuthorizationScopeView":
    case "AuthorizationScopeAggregateReceipt":
    case "AuthorizationScopeUnavailable":
    case "AuthorizationScopeDecision":
      [tag, bytes] = [1, message.requestId];
      break;
    case "CurrentRowsReceipt":
      [tag, bytes] = [2, message.receipt.requestId];
      break;
    case "ChunkUploadStart":
    case "ChunkUploadNodes":
    case "ChunkUploadResult":
      [tag, bytes] = [3, message.upload.valueRef.root.objectHash];
      break;
  }
  return toHex(concatBytes(new Uint8Array([tag]), bytes));
}
