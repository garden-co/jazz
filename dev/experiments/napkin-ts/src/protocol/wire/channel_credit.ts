import {
  CHANNEL_CHUNK_BYTES,
  CONTROL_RESERVE_BYTES,
  CONTROL_RESERVE_MESSAGES,
  INTERACTIVE_RESERVE_BYTES,
  MAX_CHANNEL_FRAME_PAYLOAD,
  MAX_CHANNEL_QUEUED_MESSAGES,
  type ChannelClass,
} from "./channels.js";
import {
  encodeFrame,
  type WireChannelCredit,
  type WireCreditKind,
  type WireInboundContext,
} from "../wire.js";
import { MAX_LOGICAL_MESSAGE_BYTES } from "../protocol_limits.js";

export const CHANNEL_FRAME_CREDIT_FLOOR = 16 * 1024;
export const channelFrameCreditCost = (encodedLen: number) =>
  Math.max(encodedLen, CHANNEL_FRAME_CREDIT_FLOOR);

const WINDOWS = [256 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024, 1024 * 1024, 1024 * 1024];
export const MAX_OUTSTANDING_CHANNEL_CREDIT = WINDOWS.reduce((a, b) => a + b, 0);

function bucket(cls: ChannelClass): number {
  switch (cls) {
    case "Control":
      return 0;
    case "Requests":
      return 1;
    case "Delivery":
      return 2;
    case "Writes":
    case "LargeValue":
      return 3;
    case "Auxiliary":
      return 4;
    case "Progress":
      return 5;
  }
}
const BUCKET_CLASSES: ChannelClass[] = [
  "Control",
  "Requests",
  "Delivery",
  "Writes",
  "Auxiliary",
  "Progress",
];

function bufferBucket(cls: ChannelClass, bytes: number): number {
  if (cls === "Auxiliary") return 3;
  if (cls === "Progress") return 5;
  if (cls === "Control") return bytes > CHANNEL_CHUNK_BYTES ? 4 : 0;
  return bytes > CHANNEL_CHUNK_BYTES ? 2 : 1;
}
const grantBufferBucket = (cls: ChannelClass, bulk: boolean) =>
  bufferBucket(cls, bulk ? CHANNEL_CHUNK_BYTES + 1 : 1);

interface BufferCost {
  bytes: number;
  count: number;
}
const costs = (): BufferCost[] => Array.from({ length: 6 }, () => ({ bytes: 0, count: 0 }));

/** Reservation of decoded-message buffer space; release() returns it as credit. */
export class BufferLease {
  private released = false;
  constructor(
    private readonly owner: ChannelCredits,
    private readonly bucket: number,
    readonly bytes: number,
  ) {}
  release(): void {
    if (this.released) return;
    this.released = true;
    this.owner.releaseBuffer(this.bucket, this.bytes);
  }
}

export class ChannelCredits {
  private outstanding = [0, 0, 0, 0, 0, 0];
  private pendingGrants = [0, 0, 0, 0, 0, 0];
  private nextSent = 0;
  private nextReceived = 0;
  private pending?: { frame: Uint8Array; index: number; amount: number; kind: WireCreditKind };
  private sentBuffers = costs();
  private receivedBuffers = costs();
  private bufferGrants = costs();
  private closed = false;
  private waker?: () => void;

  constructor(private readonly context: WireInboundContext) {}

  private wake() {
    const w = this.waker;
    this.waker = undefined;
    w?.();
  }

  private static bufferCanAdd(c: BufferCost[], index: number, bytes: number): boolean {
    if (index === 3 || index === 5)
      return (
        c[index].bytes + bytes <= MAX_LOGICAL_MESSAGE_BYTES &&
        c[index].count < (index === 5 ? CONTROL_RESERVE_MESSAGES : MAX_CHANNEL_QUEUED_MESSAGES)
      );
    const canonical = [0, 1, 2, 4];
    const totalBytes = canonical.reduce((s, i) => s + c[i].bytes, 0);
    const totalCount = canonical.reduce((s, i) => s + c[i].count, 0);
    const control = index === 0 || index === 4;
    return (
      totalBytes + bytes <=
        MAX_LOGICAL_MESSAGE_BYTES + INTERACTIVE_RESERVE_BYTES + CONTROL_RESERVE_BYTES &&
      totalCount < MAX_CHANNEL_QUEUED_MESSAGES &&
      (control ||
        (c[1].bytes + c[2].bytes + bytes <= MAX_LOGICAL_MESSAGE_BYTES + INTERACTIVE_RESERVE_BYTES &&
          c[1].count + c[2].count < MAX_CHANNEL_QUEUED_MESSAGES - CONTROL_RESERVE_MESSAGES)) &&
      ((index !== 2 && index !== 4) || c[2].bytes + c[4].bytes + bytes <= MAX_LOGICAL_MESSAGE_BYTES)
    );
  }

  canReserveMessage(cls: ChannelClass, bytes: number): boolean {
    return (
      !this.closed && ChannelCredits.bufferCanAdd(this.sentBuffers, bufferBucket(cls, bytes), bytes)
    );
  }

  reserveMessage(cls: ChannelClass, bytes: number): void {
    if (!this.canReserveMessage(cls, bytes))
      throw new Error("assertion failed: self.can_reserve_message(class, bytes)");
    const c = this.sentBuffers[bufferBucket(cls, bytes)];
    c.bytes += bytes;
    c.count += 1;
  }

  receiveMessage(cls: ChannelClass, bytes: number): BufferLease {
    const index = bufferBucket(cls, bytes);
    if (this.closed || !ChannelCredits.bufferCanAdd(this.receivedBuffers, index, bytes))
      throw new Error("decoded message receive window exceeded");
    this.receivedBuffers[index].bytes += bytes;
    this.receivedBuffers[index].count += 1;
    return new BufferLease(this, index, bytes);
  }

  /** Called when a lease is released (Rust: BufferLeaseInner::drop). */
  releaseBuffer(index: number, bytes: number): void {
    this.receivedBuffers[index].bytes -= bytes;
    this.receivedBuffers[index].count -= 1;
    if (!this.closed) {
      this.bufferGrants[index].bytes += bytes;
      this.bufferGrants[index].count += 1;
      this.wake();
    }
  }

  close(): void {
    this.closed = true;
    this.pending = undefined;
    this.pendingGrants = [0, 0, 0, 0, 0, 0];
    this.bufferGrants = costs();
    this.wake();
  }

  canSend(cls: ChannelClass): boolean {
    const i = bucket(cls);
    return WINDOWS[i] - this.outstanding[i] >= MAX_CHANNEL_FRAME_PAYLOAD + 256;
  }

  charge(cls: ChannelClass, encodedLen: number): void {
    const i = bucket(cls);
    const next = this.outstanding[i] + channelFrameCreditCost(encodedLen);
    if (next > WINDOWS[i]) throw new Error("channel sender exceeded receive credit");
    this.outstanding[i] = next;
  }

  consumed(cls: ChannelClass, encodedLen: number): void {
    const i = bucket(cls);
    const next = this.pendingGrants[i] + channelFrameCreditCost(encodedLen);
    if (next > WINDOWS[i]) throw new Error("channel receiver exceeded ungranted window");
    this.pendingGrants[i] = next;
    this.wake();
  }

  receiveCredit(grant: WireChannelCredit): void {
    try {
      this.context.validateMetadata(grant.protocolVersion, grant.features, grant.session);
    } catch (e) {
      throw new Error(`invalid credit context: ${(e as Error).message}`);
    }
    if (grant.sequence !== this.nextReceived) throw new Error("channel credit sequence mismatch");
    const index = bucket(grant.class);
    const amount = grant.consumedBytes;
    if (amount === 0) throw new Error("empty channel credit");
    const kind = grant.kind;
    if (kind.kind === "Frames") {
      if (amount > this.outstanding[index])
        throw new Error("channel credit exceeds outstanding balance");
      this.outstanding[index] -= amount;
    } else {
      const c = this.sentBuffers[grantBufferBucket(grant.class, kind.bulk)];
      if (kind.count === 0 || kind.count > c.count || amount > c.bytes)
        throw new Error("message credit exceeds outstanding balance");
      c.bytes -= amount;
      c.count -= kind.count;
    }
    this.nextReceived += 1;
    this.wake();
  }

  peekGrant(): Uint8Array | undefined {
    if (this.pending) return this.pending.frame;
    let index: number;
    let amount: number;
    let cls: ChannelClass;
    let kind: WireCreditKind;
    const bi = this.bufferGrants.findIndex((c) => c.count !== 0);
    const pi = this.pendingGrants.findIndex((a) => a !== 0);
    if (bi >= 0) {
      const c = this.bufferGrants[bi];
      [index, amount] = [bi, c.bytes];
      cls = (
        ["Control", "Requests", "Writes", "Auxiliary", "Control", "Progress"] as ChannelClass[]
      )[bi];
      kind = { kind: "Messages", count: c.count, bulk: bi === 2 || bi === 4 };
    } else if (pi >= 0) {
      [index, amount, cls, kind] = [
        pi,
        this.pendingGrants[pi],
        BUCKET_CLASSES[pi],
        { kind: "Frames" },
      ];
    } else return undefined;
    const frame = encodeFrame({
      kind: "ChannelCredit",
      credit: {
        protocolVersion: this.context.expectedProtocolVersion,
        features: this.context.negotiatedFeatures,
        session: this.context.expectedSession,
        class: cls,
        sequence: this.nextSent,
        consumedBytes: amount,
        kind,
      },
    });
    this.pending = { frame, index, amount, kind };
    return frame;
  }

  acceptGrant(): void {
    const p = this.pending;
    if (!p) throw new Error("no channel credit grant pending");
    this.pending = undefined;
    if (p.kind.kind === "Frames") {
      if (this.pendingGrants[p.index] < p.amount)
        throw new Error("channel grant accounting underflow");
      this.pendingGrants[p.index] -= p.amount;
    } else {
      this.bufferGrants[p.index].bytes -= p.amount;
      this.bufferGrants[p.index].count -= p.kind.count;
    }
    this.nextSent += 1;
  }

  hasPendingGrant(): boolean {
    return (
      !!this.pending ||
      this.pendingGrants.some((a) => a !== 0) ||
      this.bufferGrants.some((c) => c.count !== 0)
    );
  }

  /** Resolves once a grant is ready to send (Rust: poll_grant_ready). */
  grantReady(): Promise<void> {
    if (this.hasPendingGrant()) return Promise.resolve();
    return new Promise((resolve) => (this.waker = resolve));
  }

  registerWaker(waker: () => void): void {
    this.waker = waker;
  }
}
