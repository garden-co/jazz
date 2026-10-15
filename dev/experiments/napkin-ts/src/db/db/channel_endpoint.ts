// Jazz routing and semantic payloads above independent ordered byte streams.
import {
  AUXILIARY_CHANNEL,
  ChannelClass,
  OrderedChannelBackend,
  type SharedChannelCredits,
  type SyncMessage,
  TransportError,
  type WireChannelEnvelope,
  type WireError,
  type WireInboundContext,
  encodeSyncMessageForFeatures,
} from "../../_extern/db-rest-wasm.js";
import { type ReceivedSyncMessage, RoutedMessages } from "./routed_messages.js";

export class ChannelEndpoint {
  routing = new RoutedMessages();
  raw: any;
  lastWireError: WireError | undefined;
  constructor(public context: WireInboundContext) {
    this.raw = new OrderedChannelBackend(context);
  }
  setTrustedEncoder(trusted: boolean): void {
    this.context.setTrustedEncoder(trusted);
  }
  incompleteReceiveTimeoutMs(): number | undefined {
    return this.raw.incompleteReceiveTimeoutMs();
  }
  expire(): void {
    this.raw.expire();
  }
  enqueue(
    channel: number,
    generation: number,
    klass: any,
    message: SyncMessage,
    barrier: boolean,
  ): void {
    let bytes: Uint8Array;
    try {
      bytes = encodeSyncMessageForFeatures(message, this.context.negotiatedFeatures());
    } catch (error) {
      throw new TransportError("Failed", `cannot encode channel message: ${String(error)}`);
    }
    if (klass === ChannelClass.Auxiliary)
      return this.raw.enqueue(channel, generation, klass, bytes);
    try {
      bytes = this.routing.prepare(channel, barrier, bytes);
    } catch (e) {
      throw new TransportError("Failed", String(e));
    }
    this.raw.enqueue(channel, generation, klass, bytes);
    this.routing.accepted(channel, barrier);
  }
  channelCredits(): SharedChannelCredits {
    return this.raw.channelCredits();
  }
  isIdle(channel: number): boolean {
    return this.raw.isIdle(channel);
  }
  nextIdleGeneration(channel: number): number {
    return this.raw.nextIdleGeneration(channel);
  }
  hasPending(): boolean {
    return this.raw.hasPending();
  }
  peekOutbound(): Uint8Array | undefined {
    return this.raw.peekOutbound();
  }
  acceptOutbound(): boolean {
    return this.raw.acceptOutbound();
  }
  pop(): ReceivedSyncMessage | undefined {
    return this.routing.pop();
  }
  receive(frame: WireChannelEnvelope, encodedLen: number): ReceivedSyncMessage | undefined {
    const received = this.raw.receive(frame, encodedLen);
    if (received) {
      if (received.class === ChannelClass.Auxiliary) {
        let message: SyncMessage;
        try {
          message = this.context.decodeSemanticPayload(received.payload);
        } catch (e) {
          this.lastWireError = e as WireError;
          throw `invalid auxiliary payload: ${String(e)}`;
        }
        if (messageClass(message)[0] !== ChannelClass.Auxiliary) throw "message is not auxiliary";
        return {
          message,
          lease: received.lease,
          receiptsValidated: this.context.validatesReceipts(),
        };
      }
      try {
        this.routing.receive(
          received.channel,
          received.class,
          received.payload,
          received.lease,
          this.context,
        );
      } catch (e) {
        this.lastWireError = e as WireError;
        throw `invalid routed payload: ${String(e)}`;
      }
    }
    return this.routing.pop();
  }
}

export function messageClass(message: SyncMessage): [any, boolean] {
  switch (message.kind) {
    case "ChunkRequestBatch":
    case "ChunkResponseBatch":
      return [ChannelClass.Auxiliary, false];
    case "SessionClaims":
    case "PublishSchema":
    case "PublishSchemaWithLens":
    case "PublishLens":
    case "CatalogueAck":
    case "CatalogueSnapshot":
      return [ChannelClass.Control, true];
    // A preceding delivery may introduce this transaction.
    case "FateUpdate":
      return [ChannelClass.Writes, true];
    case "CommitUnit":
      return [ChannelClass.Writes, false];
    case "RegisterShape":
    case "Subscribe":
    case "Unsubscribe":
    case "AuthorizationScopeIntent":
    case "CurrentRowsRequest":
    case "CurrentRowsCancel":
    case "ReadBarrier":
      return [ChannelClass.Requests, false];
    case "ChunkUploadStart":
    case "ChunkUploadNodes":
    case "ChunkUploadResult":
      return [ChannelClass.LargeValue, false];
    default:
      return [ChannelClass.Delivery, false];
  }
}

/** The fixed lock-independent auxiliary channel. */
export class AuxiliaryChannelEndpoint {
  endpoint: ChannelEndpoint;
  message: SyncMessage | undefined;
  pumpOwned = false;
  waker: (() => void) | undefined;
  constructor(context: WireInboundContext) {
    this.endpoint = new ChannelEndpoint(context);
  }
  setTrustedEncoder(trusted: boolean): void {
    this.endpoint.setTrustedEncoder(trusted);
  }
  lastWireError(): WireError | undefined {
    return this.endpoint.lastWireError;
  }
  incompleteReceiveTimeoutMs(): number | undefined {
    return this.endpoint.incompleteReceiveTimeoutMs();
  }
  expireIncompleteReceive(): void {
    this.endpoint.expire();
  }
  setChannelCredits(credits: SharedChannelCredits): void {
    this.endpoint.raw.setChannelCredits(credits);
  }
  channelCredits(): SharedChannelCredits {
    return this.endpoint.channelCredits();
  }
  outboundIsReady(): boolean {
    return (
      this.hasPendingOutbound() && this.endpoint.channelCredits().canSend(ChannelClass.Auxiliary)
    );
  }
  setPumpOwned(): void {
    this.pumpOwned = true;
  }
  enqueue(message: SyncMessage): void {
    const rejected = this.tryEnqueue(message);
    if (rejected) throw rejected[0];
  }
  /** Hands an unadmitted message back to its caller. */
  tryEnqueue(message: SyncMessage): [Error, SyncMessage] | undefined {
    if (messageClass(message)[0] !== ChannelClass.Auxiliary)
      return [
        new TransportError("Failed", "canonical message attempted auxiliary bypass"),
        message,
      ];
    if (this.message !== undefined) return [new TransportError("Backpressure"), message];
    try {
      this.endpoint.enqueue(AUXILIARY_CHANNEL, 0, ChannelClass.Auxiliary, message, false);
    } catch (error) {
      return [error as Error, message];
    }
    this.message = message;
    const waker = this.waker;
    this.waker = undefined;
    waker?.();
    return undefined;
  }
  peekOutbound(): Uint8Array | undefined {
    return this.endpoint.peekOutbound();
  }
  acceptOutbound(): SyncMessage | undefined {
    if (!this.endpoint.acceptOutbound()) return undefined;
    const m = this.message;
    this.message = undefined;
    return m;
  }
  hasPendingOutbound(): boolean {
    return this.endpoint.hasPending();
  }
  /** Ready now, or registers `wake` for newly admitted output / credit. */
  pollOutboundReady(wake: () => void): boolean {
    if (this.outboundIsReady()) return true;
    this.waker = wake;
    this.endpoint.channelCredits().registerWaker(wake);
    return false;
  }
  receive(frame: WireChannelEnvelope, encodedLen: number): SyncMessage | undefined {
    if (frame.extent.channel !== AUXILIARY_CHANNEL || frame.extent.class !== ChannelClass.Auxiliary)
      throw "non-auxiliary frame attempted auxiliary bypass";
    return this.endpoint.receive(frame, encodedLen)?.message;
  }
}
export type SharedAuxiliaryEndpoint = AuxiliaryChannelEndpoint;
