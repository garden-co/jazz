// Jazz message dependencies above opaque reliable ordered byte streams.
import { Reader, Writer } from "../../support/postcard.js";
import {
  type BufferLease,
  CONTROL_RESERVE_MESSAGES,
  MAX_CHANNELS,
  MAX_CHANNEL_QUEUED_MESSAGES,
  MAX_LOGICAL_MESSAGE_BYTES,
  type SyncMessage,
  type WireInboundContext,
  WireError,
} from "../../_extern/db-rest-wasm.js";
import { messageClass } from "./channel_endpoint.js";

export const ENVELOPE_VERSION = 1;
export const MAX_ENVELOPE_OVERHEAD = 1024;

type Predecessors = [number, number][] | undefined;
interface Envelope {
  version: number;
  epoch: number;
  ordinal: number;
  predecessors: Predecessors;
  payload: Uint8Array;
}

function encodeEnvelope(e: Envelope): Uint8Array {
  const w = new Writer().u8(e.version).varint(e.epoch).varint(e.ordinal);
  w.option(e.predecessors, (w, deps) => w.seq(deps, (w, [slot, n]) => w.varint(slot).varint(n)));
  return w.bytes(e.payload).finish();
}

function decodeEnvelope(bytes: Uint8Array): Envelope {
  let env: Envelope;
  const r = new Reader(bytes);
  try {
    env = {
      version: r.u8(),
      epoch: r.varint(),
      ordinal: r.varint(),
      predecessors: r.option((r) => r.seq((r) => [r.varint(), r.varint()] as [number, number])),
      payload: r.bytes(),
    };
  } catch (error) {
    throw `invalid routed envelope: ${(error as Error).message}`;
  }
  if (r.remaining !== 0) throw "trailing bytes after routed envelope";
  return env;
}

export function maxRoutedPayloadBytes(): number {
  return MAX_LOGICAL_MESSAGE_BYTES - MAX_ENVELOPE_OVERHEAD;
}

export interface ReceivedSyncMessage {
  message: SyncMessage;
  lease?: BufferLease;
  receiptsValidated: boolean;
}
export const unleasedMessage = (message: SyncMessage): ReceivedSyncMessage => ({
  message,
  lease: undefined,
  receiptsValidated: false,
});

interface Pending {
  channel: number;
  epoch: number;
  ordinal: number;
  predecessors: Predecessors;
  message: SyncMessage;
  lease: BufferLease;
  receiptsValidated: boolean;
}

export class RoutedMessages {
  sentEpoch = 0;
  sent = new Array<number>(MAX_CHANNELS).fill(0);
  receiveEpoch = 0;
  received = new Array<number>(MAX_CHANNELS).fill(0);
  admitted = new Array<number>(MAX_CHANNELS).fill(0);
  pending: Pending[] = [];
  ready: ReceivedSyncMessage[] = [];

  /** Read-only: failed byte-queue admission cannot consume an epoch or ordinal. */
  prepare(channel: number, barrier: boolean, payload: Uint8Array): Uint8Array {
    if (channel >= MAX_CHANNELS) throw "invalid logical stream ID";
    if (payload.length > maxRoutedPayloadBytes())
      throw "semantic message exceeds routed payload limit";
    const ordinal = this.sent[channel] + 1;
    if (!Number.isSafeInteger(ordinal)) throw "logical ordinal exhausted";
    if (barrier && !Number.isSafeInteger(this.sentEpoch + 1)) throw "logical epoch exhausted";
    const predecessors: Predecessors = barrier
      ? this.sent.flatMap((n, slot) => (n !== 0 ? [[slot, n] as [number, number]] : []))
      : undefined;
    return encodeEnvelope({
      version: ENVELOPE_VERSION,
      epoch: this.sentEpoch,
      ordinal,
      predecessors,
      payload,
    });
  }

  accepted(channel: number, barrier: boolean): void {
    this.sent[channel] += 1;
    if (barrier) this.sentEpoch += 1;
  }

  receive(
    channel: number,
    klass: any,
    bytes: Uint8Array,
    lease: BufferLease,
    context: WireInboundContext,
  ): void {
    const malformed = (message: string) => new WireError("MalformedFrame", "Never", message);
    let envelope: Envelope;
    try {
      envelope = decodeEnvelope(bytes);
    } catch (e) {
      throw malformed(String(e));
    }
    const slot = channel;
    if (
      envelope.version !== ENVELOPE_VERSION ||
      envelope.epoch < this.receiveEpoch ||
      envelope.epoch - this.receiveEpoch > MAX_CHANNEL_QUEUED_MESSAGES ||
      envelope.ordinal !== this.received[slot] + 1
    )
      throw malformed("invalid routed epoch or stream ordinal");
    const deps = envelope.predecessors;
    if (deps) {
      let bad = deps.length > MAX_CHANNELS;
      for (let i = 1; i < deps.length && !bad; i++) bad = deps[i - 1][0] >= deps[i][0];
      if (bad || deps.some(([s, o]) => s >= MAX_CHANNELS || o === 0))
        throw malformed("invalid barrier dependency vector");
    }
    if (
      this.pending.length + this.ready.length >=
      MAX_CHANNEL_QUEUED_MESSAGES + CONTROL_RESERVE_MESSAGES
    )
      throw malformed("routed message queue count exceeded");
    const message = context.decodeSemanticPayload(envelope.payload);
    const [expectedClass, barrier] = messageClass(message);
    if (klass !== expectedClass || barrier !== (deps !== undefined))
      throw malformed("message routing metadata disagrees with semantics");
    this.received[slot] = envelope.ordinal;
    this.pending.push({
      channel,
      epoch: envelope.epoch,
      ordinal: envelope.ordinal,
      predecessors: deps,
      message,
      lease,
      receiptsValidated: context.validatesReceipts(),
    });
    for (;;) {
      const index = this.pending.findIndex(
        (p) =>
          p.epoch === this.receiveEpoch &&
          p.ordinal === this.admitted[p.channel] + 1 &&
          (p.predecessors === undefined || p.predecessors.every(([s, n]) => this.admitted[s] >= n)),
      );
      if (index < 0) break;
      const [p] = this.pending.splice(index, 1);
      this.admitted[p.channel] = p.ordinal;
      if (p.predecessors) this.receiveEpoch += 1;
      this.ready.push({
        message: p.message,
        lease: p.lease,
        receiptsValidated: p.receiptsValidated,
      });
    }
  }

  pop(): ReceivedSyncMessage | undefined {
    return this.ready.shift();
  }
}
