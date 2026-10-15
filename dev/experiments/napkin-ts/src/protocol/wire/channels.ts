import { Reader, Writer } from "../../support/postcard.js";
import { SortedMap, cmpNum } from "../../support/collections.js";
import { MAX_LOGICAL_MESSAGE_BYTES } from "../protocol_limits.js";

export const MAX_CHANNELS = 64;
export const CHANNEL_CHUNK_BYTES = 64 * 1024;
export const MAX_CHANNEL_FRAME_PAYLOAD = CHANNEL_CHUNK_BYTES + CHANNEL_CHUNK_BYTES / 10 + 64;
export const CONTROL_RESERVE_BYTES = 1024 * 1024;
export const INTERACTIVE_RESERVE_BYTES = 8 * 1024 * 1024;
export const MAX_CHANNEL_BUFFER_BYTES =
  2 * MAX_LOGICAL_MESSAGE_BYTES + INTERACTIVE_RESERVE_BYTES + CONTROL_RESERVE_BYTES;
export const MAX_CHANNEL_QUEUED_MESSAGES = 1024;
export const CONTROL_RESERVE_MESSAGES = 8;
export const CHANNEL_HEADER_BYTES = 64;
export const CONTROL_CHANNEL = 0;
export const AUXILIARY_CHANNEL = 63;
export const PROGRESS_CHANNEL = 62;

export type ChannelClass =
  | "Control"
  | "Requests"
  | "Delivery"
  | "Writes"
  | "LargeValue"
  | "Auxiliary"
  | "Progress";
export const CHANNEL_CLASSES: ChannelClass[] = [
  "Control",
  "Requests",
  "Delivery",
  "Writes",
  "LargeValue",
  "Auxiliary",
  "Progress",
];

export interface ChannelFrame {
  channel: number;
  generation: number;
  sequence: number;
  class: ChannelClass;
  first: boolean;
  last: boolean;
  messageLen: number;
  decodedLen: number;
  payload: Uint8Array;
}

export function writeChannelFrame(w: Writer, f: ChannelFrame): void {
  w.varint(f.channel)
    .varint(f.generation)
    .varint(f.sequence)
    .variant(CHANNEL_CLASSES.indexOf(f.class));
  w.bool(f.first).bool(f.last).varint(f.messageLen).varint(f.decodedLen).bytes(f.payload);
}
export function readChannelFrame(r: Reader): ChannelFrame {
  return {
    channel: r.varint(),
    generation: r.varint(),
    sequence: r.varint(),
    class: CHANNEL_CLASSES[r.variant(7, "ChannelClass")],
    first: r.bool(),
    last: r.bool(),
    messageLen: r.varint(),
    decodedLen: r.varint(),
    payload: r.bytes().slice(),
  };
}

export function validateChannelFrame(f: ChannelFrame): void {
  if (f.channel >= MAX_CHANNELS) throw new Error("channel slot exceeds connection limit");
  if ((f.channel === AUXILIARY_CHANNEL) !== (f.class === "Auxiliary"))
    throw new Error("auxiliary class must use reserved auxiliary channel");
  if ((f.channel === PROGRESS_CHANNEL) !== (f.class === "Progress"))
    throw new Error("progress class must use reserved progress channel");
  if ((f.channel === CONTROL_CHANNEL) !== (f.class === "Control"))
    throw new Error("control class must use the reserved channel");
  if (f.decodedLen > CHANNEL_CHUNK_BYTES || f.payload.length > MAX_CHANNEL_FRAME_PAYLOAD)
    throw new Error("channel extent exceeds chunk limit");
  if (f.first) {
    if (f.messageLen === 0 || f.messageLen > MAX_LOGICAL_MESSAGE_BYTES)
      throw new Error("channel message exceeds logical size limit");
    if (f.decodedLen > f.messageLen)
      throw new Error("channel extent exceeds declared message size");
  } else if (f.messageLen !== 0) throw new Error("continuation repeats logical size");
  if (f.decodedLen === 0 || f.payload.length === 0) throw new Error("empty channel extent");
}

export function encodeChannelFrame(f: ChannelFrame): Uint8Array {
  validateChannelFrame(f);
  const w = new Writer();
  writeChannelFrame(w, f);
  return w.finish();
}

export function decodeChannelFrame(bytes: Uint8Array): ChannelFrame {
  if (bytes.length > CHANNEL_HEADER_BYTES + MAX_CHANNEL_FRAME_PAYLOAD)
    throw new Error("channel frame exceeds size limit");
  const r = new Reader(bytes);
  const f = readChannelFrame(r);
  if (r.remaining) throw new Error("trailing bytes after channel frame");
  validateChannelFrame(f);
  return f;
}

interface Message {
  bytes: Uint8Array;
  offset: number;
}
interface OutboundChannel {
  class: ChannelClass;
  generation: number;
  sequence: number;
  bytes: number;
  messages: Message[];
}
export interface ScheduledChunk {
  channel: number;
  generation: number;
  sequence: number;
  class: ChannelClass;
  first: boolean;
  last: boolean;
  messageLen: number;
  bytes: Uint8Array;
}

const ROUND: ChannelClass[] = [
  ...Array<ChannelClass>(8).fill("Control"),
  ...Array<ChannelClass>(4).fill("Requests"),
  "Delivery",
  "Delivery",
  "Writes",
  "Writes",
  "LargeValue",
  "Auxiliary",
  "Progress",
  "Progress",
];

/** Weighted round-robin scheduler of logical messages into channel extents. */
export class ChannelScheduler {
  private channels = new SortedMap<number, OutboundChannel>(cmpNum);
  private roundCursor = 0;
  private classCursor = [0, 0, 0, 0, 0, 0, 0];
  private selected?: number;
  private bytes = 0;
  private dataBytes = 0;
  private messages = 0;
  private dataMessages = 0;
  private bulkBytes = 0;
  private progressBytes = 0;
  private progressMessages = 0;

  enqueue(channel: number, generation: number, cls: ChannelClass, payload: Uint8Array): void {
    if (
      channel >= MAX_CHANNELS ||
      (channel === CONTROL_CHANNEL) !== (cls === "Control") ||
      (channel === AUXILIARY_CHANNEL) !== (cls === "Auxiliary") ||
      (channel === PROGRESS_CHANNEL) !== (cls === "Progress")
    )
      throw new Error("invalid channel slot or class");
    const len = payload.length;
    if (len === 0 || len > MAX_LOGICAL_MESSAGE_BYTES)
      throw new Error("invalid logical message size");
    if (cls === "Progress") {
      if (
        this.progressMessages >= CONTROL_RESERVE_MESSAGES ||
        this.progressBytes + len > MAX_LOGICAL_MESSAGE_BYTES
      )
        throw new Error("progress channel queue backpressure");
    } else if (
      this.messages - this.progressMessages >= MAX_CHANNEL_QUEUED_MESSAGES ||
      (cls !== "Control" &&
        this.dataMessages >= MAX_CHANNEL_QUEUED_MESSAGES - CONTROL_RESERVE_MESSAGES) ||
      this.bytes - this.progressBytes + len >
        MAX_CHANNEL_BUFFER_BYTES - MAX_LOGICAL_MESSAGE_BYTES ||
      (cls !== "Control" &&
        this.dataBytes + len > MAX_LOGICAL_MESSAGE_BYTES + INTERACTIVE_RESERVE_BYTES) ||
      (len > CHANNEL_CHUNK_BYTES && this.bulkBytes + len > MAX_LOGICAL_MESSAGE_BYTES)
    )
      throw new Error("channel queue backpressure");
    const existing = this.channels.get(channel);
    if (existing) {
      if (
        (existing.generation !== generation || existing.class !== cls) &&
        !(this.isIdle(channel) && existing.generation + 1 === generation)
      )
        throw new Error("channel generation or class changed without reset");
      if (existing.bytes + len > MAX_LOGICAL_MESSAGE_BYTES)
        throw new Error("channel queue backpressure");
    }
    const state = this.channels.getOrInsert(channel, () => ({
      class: cls,
      generation,
      sequence: 0,
      bytes: 0,
      messages: [],
    }));
    if (state.generation !== generation) {
      state.generation = generation;
      state.sequence = 0;
      state.class = cls;
    }
    state.bytes += len;
    state.messages.push({ bytes: payload, offset: 0 });
    this.bytes += len;
    if (cls === "Progress") {
      this.progressBytes += len;
      this.progressMessages += 1;
    } else if (len > CHANNEL_CHUNK_BYTES) this.bulkBytes += len;
    if (cls !== "Control" && cls !== "Progress") {
      this.dataBytes += len;
      this.dataMessages += 1;
    }
    this.messages += 1;
  }

  nextChunk(): ScheduledChunk | undefined {
    return this.nextChunkWhere(() => true);
  }

  nextChunkWhere(hasCredit: (c: ChannelClass) => boolean): ScheduledChunk | undefined {
    if (this.selected === undefined) {
      const eligible = (c: OutboundChannel) => hasCredit(c.class) && c.messages.length > 0;
      for (let i = 0; i < ROUND.length; i++) {
        const cls = ROUND[this.roundCursor];
        this.roundCursor = (this.roundCursor + 1) % ROUND.length;
        const ci = CHANNEL_CLASSES.indexOf(cls);
        const after = this.classCursor[ci];
        let candidate: number | undefined;
        let wrap: number | undefined;
        for (const [id, state] of this.channels) {
          if (state.class !== cls || !eligible(state)) continue;
          wrap ??= id;
          if (id > after) {
            candidate = id;
            break;
          }
        }
        candidate ??= wrap;
        if (candidate !== undefined) {
          this.classCursor[ci] = candidate;
          this.selected = candidate;
          break;
        }
      }
    }
    if (this.selected === undefined) return undefined;
    const channel = this.selected;
    const state = this.channels.get(channel);
    const message = state?.messages[0];
    if (!state || !message) return undefined;
    const end = Math.min(message.offset + CHANNEL_CHUNK_BYTES, message.bytes.length);
    return {
      channel,
      generation: state.generation,
      sequence: state.sequence,
      class: state.class,
      first: message.offset === 0,
      last: end === message.bytes.length,
      messageLen: message.offset === 0 ? message.bytes.length : 0,
      bytes: message.bytes.subarray(message.offset, end),
    };
  }

  accepted(): void {
    const channel = this.selected;
    if (channel === undefined) throw new Error("no selected channel extent");
    const state = this.channels.get(channel)!;
    const message = state.messages[0];
    message.offset += Math.min(CHANNEL_CHUNK_BYTES, message.bytes.length - message.offset);
    state.sequence += 1;
    if (message.offset === message.bytes.length) {
      const len = message.bytes.length;
      state.messages.shift();
      state.bytes -= len;
      this.bytes -= len;
      if (state.class === "Progress") {
        this.progressBytes -= len;
        this.progressMessages -= 1;
      } else if (len > CHANNEL_CHUNK_BYTES) this.bulkBytes -= len;
      if (state.class !== "Control" && state.class !== "Progress") {
        this.dataBytes -= len;
        this.dataMessages -= 1;
      }
      this.messages -= 1;
    }
    this.selected = undefined;
  }

  nextIdleGeneration(channel: number): number {
    if (!this.isIdle(channel)) throw new Error("channel still owns queued or selected messages");
    const state = this.channels.get(channel);
    return state ? state.generation + 1 : 0;
  }

  isIdle(channel: number): boolean {
    const state = this.channels.get(channel);
    return this.selected !== channel && (!state || state.messages.length === 0);
  }

  queuedBytes(): number {
    return this.bytes;
  }
  queuedMessages(): number {
    return this.messages;
  }
}
