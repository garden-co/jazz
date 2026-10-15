import { SortedMap, cmpNum } from "../../support/collections.js";
import { monotonicMs } from "../../support/runtime.js";
import { StreamDecoder, StreamEncoder, type Codec } from "../../compression/stream.js";
import {
  MAX_ENCODED_MESSAGE_BYTES,
  MAX_FRAGMENT_REASSEMBLY_AGE_MS,
  MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
} from "../protocol_limits.js";
import { BufferLease, ChannelCredits } from "./channel_credit.js";
import {
  ChannelScheduler,
  MAX_CHANNEL_BUFFER_BYTES,
  MAX_CHANNEL_FRAME_PAYLOAD,
  MAX_CHANNELS,
  validateChannelFrame,
  type ChannelClass,
  type ChannelFrame,
} from "./channels.js";
import {
  FEATURE_PAYLOAD_LZ4,
  FEATURE_PAYLOAD_ZSTD,
  TransportError,
  compressionFeature,
  compressionFromFeatures,
  encodeFrame,
  type WireChannelEnvelope,
  type WireFeatures,
  type WireInboundContext,
} from "../wire.js";

const CODEC_MASK = FEATURE_PAYLOAD_LZ4 | FEATURE_PAYLOAD_ZSTD;

function codec(features: WireFeatures): Codec | undefined {
  switch (features & CODEC_MASK) {
    case 0n:
      return undefined;
    case FEATURE_PAYLOAD_LZ4:
      return "Lz4";
    case FEATURE_PAYLOAD_ZSTD:
      return "Zstd";
    default:
      throw new Error("channel declares multiple compression codecs");
  }
}

interface InboundChannel {
  generation: number;
  sequence: number;
  class: ChannelClass;
  activeFeatures: WireFeatures;
  decoder?: StreamDecoder;
  messageLen: number;
  encodedLen: number;
  started?: number;
  progressed?: number;
  parts: Uint8Array[];
  payloadLen: number;
  lease?: BufferLease;
}

export interface ReceivedBytes {
  channel: number;
  class: ChannelClass;
  payload: Uint8Array;
  lease: BufferLease;
}

const concat = (parts: Uint8Array[], len: number): Uint8Array => {
  const out = new Uint8Array(len);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
};

/** Ordered channel transport: fragments, compresses and reassembles logical messages. */
export class OrderedChannelBackend {
  private credits: ChannelCredits;
  private scheduler = new ChannelScheduler();
  private encoders = new SortedMap<number, [number, StreamEncoder | undefined]>(cmpNum);
  private inbound = new SortedMap<number, InboundChannel>(cmpNum);
  private outboundFeatures: WireFeatures;
  private reserved = 0;
  private pending?: Uint8Array;
  private pendingLast = false;
  private pendingClass: ChannelClass = "Control";
  private failed?: string;
  private idleTimeoutMs = MAX_FRAGMENT_REASSEMBLY_IDLE_MS;
  private ageTimeoutMs = MAX_FRAGMENT_REASSEMBLY_AGE_MS;

  constructor(private readonly context: WireInboundContext) {
    const features = context.negotiatedFeatures;
    const compression = compressionFromFeatures(features);
    // The browser build has no zstd encoder compiled in.
    const selected = compression === "Zstd" ? 0n : compressionFeature(compression);
    this.outboundFeatures = (features & ~CODEC_MASK) | selected;
    this.credits = new ChannelCredits(context.clone());
  }

  incompleteReceiveTimeoutMs(): number | undefined {
    const now = monotonicMs();
    let min: number | undefined;
    for (const [, s] of this.inbound) {
      for (const [at, limit] of [
        [s.started, this.ageTimeoutMs],
        [s.progressed, this.idleTimeoutMs],
      ] as const) {
        if (at === undefined) continue;
        const remaining = Math.ceil(Math.max(0, limit - (now - at)));
        min = min === undefined ? remaining : Math.min(min, remaining);
      }
    }
    return min;
  }

  expire(): void {
    const now = monotonicMs();
    let expired = false;
    for (const [, s] of this.inbound)
      if (
        (s.started !== undefined && now - s.started >= this.ageTimeoutMs) ||
        (s.progressed !== undefined && now - s.progressed >= this.idleTimeoutMs)
      )
        expired = true;
    if (expired) {
      this.inbound.clear();
      this.reserved = 0;
      const error = "incomplete channel message expired; reconnect required";
      this.failed = error;
      throw new Error(error);
    }
  }

  enqueue(channel: number, generation: number, cls: ChannelClass, payload: Uint8Array): void {
    try {
      this.expire();
    } catch (e) {
      throw new TransportError("Failed", (e as Error).message);
    }
    if (this.failed !== undefined) throw new TransportError("Failed", this.failed);
    const size = payload.length;
    if (!this.credits.canReserveMessage(cls, size)) throw new TransportError("Backpressure");
    try {
      this.scheduler.enqueue(channel, generation, cls, payload);
    } catch (e) {
      const error = (e as Error).message;
      throw error.includes("backpressure")
        ? new TransportError("Backpressure")
        : new TransportError("Failed", error);
    }
    this.credits.reserveMessage(cls, size);
    const enc = this.encoders.get(channel);
    if (enc && enc[0] !== generation) this.encoders.delete(channel);
  }

  setChannelCredits(credits: ChannelCredits): void {
    this.credits = credits;
  }
  channelCredits(): ChannelCredits {
    return this.credits;
  }
  isIdle(channel: number): boolean {
    return this.scheduler.isIdle(channel);
  }
  nextIdleGeneration(channel: number): number {
    return this.scheduler.nextIdleGeneration(channel);
  }
  hasPending(): boolean {
    return this.scheduler.queuedMessages() !== 0;
  }

  peekOutbound(): Uint8Array | undefined {
    this.expire();
    if (this.failed !== undefined) throw new Error(this.failed);
    if (this.pending) return this.pending;
    const chunk = this.scheduler.nextChunkWhere((cls) => this.credits.canSend(cls));
    if (!chunk) return undefined;
    const c = codec(this.outboundFeatures);
    if (!this.encoders.has(chunk.channel))
      this.encoders.set(chunk.channel, [chunk.generation, c ? new StreamEncoder(c) : undefined]);
    const [generation, encoder] = this.encoders.get(chunk.channel)!;
    if (generation !== chunk.generation) throw new Error("outbound codec generation mismatch");
    let payload: Uint8Array;
    if (encoder) {
      let output = new Uint8Array(encodedExtentCapacity(chunk.bytes.length));
      let consumed = 0;
      let written = 0;
      while (consumed < chunk.bytes.length) {
        const p = encoder.encode(chunk.bytes.subarray(consumed), output.subarray(written));
        if (p.consumed === 0 && p.written === 0) {
          const grown = growEncodedExtent(output);
          if (!grown) throw new Error("channel encoder exceeded physical extent bound");
          output = grown;
          continue;
        }
        consumed += p.consumed;
        written += p.written;
      }
      for (;;) {
        const p = encoder.flush(output.subarray(written));
        written += p.written;
        if (p.finished) break;
        if (p.written === 0) {
          const grown = growEncodedExtent(output);
          if (!grown) throw new Error("channel flush exceeded physical extent bound");
          output = grown;
        }
      }
      payload = output.slice(0, written);
    } else payload = chunk.bytes.slice();
    const extent: ChannelFrame = {
      channel: chunk.channel,
      generation: chunk.generation,
      sequence: chunk.sequence,
      class: chunk.class,
      first: chunk.first,
      last: chunk.last,
      messageLen: chunk.messageLen,
      decodedLen: chunk.bytes.length,
      payload,
    };
    validateChannelFrame(extent);
    this.pendingLast = extent.last;
    this.pendingClass = extent.class;
    const frame = encodeFrame({
      kind: "Channel",
      envelope: {
        protocolVersion: this.context.expectedProtocolVersion,
        features: this.outboundFeatures,
        session: this.context.expectedSession,
        extent,
      },
    });
    this.pending = frame;
    return frame;
  }

  acceptOutbound(): boolean {
    if (!this.pending) throw new Error("no encoded channel frame to accept");
    this.credits.charge(this.pendingClass, this.pending.length);
    this.scheduler.accepted();
    this.pending = undefined;
    return this.pendingLast;
  }

  receive(frame: WireChannelEnvelope, encodedLen: number): ReceivedBytes | undefined {
    this.expire();
    if (this.failed !== undefined) throw new Error(this.failed);
    this.credits.consumed(frame.extent.class, encodedLen);
    try {
      return this.receiveInner(frame);
    } catch (e) {
      this.inbound.clear();
      this.reserved = 0;
      this.failed = (e as Error).message;
      throw e;
    }
  }

  private receiveInner(frame: WireChannelEnvelope): ReceivedBytes | undefined {
    try {
      this.context.validateChannelMetadata(frame);
    } catch (e) {
      throw new Error(String(e));
    }
    const extent = frame.extent;
    validateChannelFrame(extent);
    const activeFeatures = frame.features & CODEC_MASK;
    const selected = codec(activeFeatures);
    const existing = this.inbound.get(extent.channel);
    let reset: boolean;
    if (!existing) {
      if (extent.generation !== 0) throw new Error("new channel must start at generation zero");
      reset = true;
    } else if (existing.generation !== extent.generation) {
      if (existing.generation + 1 !== extent.generation || existing.messageLen !== 0)
        throw new Error("stale or incomplete channel generation reset");
      reset = true;
    } else reset = false;
    if (reset) {
      if (!extent.first || extent.sequence !== 0)
        throw new Error("new channel generation must start at sequence zero");
      if (this.inbound.size === MAX_CHANNELS && !this.inbound.has(extent.channel))
        throw new Error("too many channel contexts");
      this.inbound.set(extent.channel, {
        generation: extent.generation,
        sequence: 0,
        class: extent.class,
        activeFeatures,
        decoder: selected ? new StreamDecoder(selected) : undefined,
        messageLen: 0,
        encodedLen: 0,
        parts: [],
        payloadLen: 0,
      });
    }
    const state = this.inbound.get(extent.channel)!;
    if (
      state.sequence !== extent.sequence ||
      state.class !== extent.class ||
      state.activeFeatures !== activeFeatures
    )
      throw new Error("channel sequence, class or compression changed");
    if (extent.first) {
      if (state.messageLen !== 0)
        throw new Error("channel starts message before predecessor completes");
      const size = extent.messageLen;
      if (this.reserved + size > MAX_CHANNEL_BUFFER_BYTES)
        throw new Error("channel declared messages exceed aggregate budget");
      state.lease = this.credits.receiveMessage(extent.class, size);
      this.reserved += size;
      state.messageLen = size;
      state.started = monotonicMs();
      state.encodedLen = 0;
    } else if (state.messageLen === 0) throw new Error("channel continuation has no message");
    state.encodedLen += extent.payload.length;
    if (state.encodedLen > MAX_ENCODED_MESSAGE_BYTES)
      throw new Error("channel encoded message exceeds size limit");
    const expected = extent.decodedLen;
    if (state.payloadLen + expected > state.messageLen)
      throw new Error("channel exceeds declared logical length");
    if (state.decoder) {
      const output = new Uint8Array(expected + 1);
      let consumed = 0;
      let written = 0;
      for (;;) {
        const p = state.decoder.decode(extent.payload.subarray(consumed), output.subarray(written));
        consumed += p.consumed;
        written += p.written;
        if (written > expected || p.finished)
          throw new Error("channel codec ended or exceeded declared decoded extent");
        if (p.consumed === 0 && p.written === 0) break;
      }
      if (consumed !== extent.payload.length || written !== expected)
        throw new Error("channel flush did not produce exact declared decoded extent");
      state.parts.push(output.subarray(0, written));
      state.payloadLen += written;
    } else {
      if (extent.payload.length !== expected)
        throw new Error("uncompressed channel extent size mismatch");
      state.parts.push(extent.payload);
      state.payloadLen += expected;
    }
    state.progressed = monotonicMs();
    state.sequence += 1;
    if (!extent.last) {
      if (state.payloadLen === state.messageLen)
        throw new Error("channel omitted final extent flag");
      return undefined;
    }
    if (state.payloadLen !== state.messageLen)
      throw new Error("channel completed before declared logical length");
    this.reserved -= state.messageLen;
    const payload = concat(state.parts, state.payloadLen);
    state.messageLen = 0;
    state.started = undefined;
    state.progressed = undefined;
    state.encodedLen = 0;
    state.parts = [];
    state.payloadLen = 0;
    const lease = state.lease;
    if (!lease) throw new Error("active receive owns buffer reservation");
    state.lease = undefined;
    return { channel: extent.channel, class: state.class, payload, lease };
  }

  /** Rust: Drop. */
  close(): void {
    this.credits.close();
  }
}

function encodedExtentCapacity(chunkLen: number): number {
  return Math.min(chunkLen + Math.floor(chunkLen / 10) + 64, MAX_CHANNEL_FRAME_PAYLOAD);
}

function growEncodedExtent(output: Uint8Array): Uint8Array | undefined {
  if (output.length >= MAX_CHANNEL_FRAME_PAYLOAD) return undefined;
  const grown = new Uint8Array(Math.min(output.length * 2, MAX_CHANNEL_FRAME_PAYLOAD));
  grown.set(output);
  return grown;
}
