import type { Transport } from "../runtime/native-runtime/native-runtime-adapter.js";
import type { PeerTransportRuntime } from "../runtime/native-runtime/browser-worker-transport.js";

/** The only JavaScript authority needed to use a platform-admitted relay. */
export type NativeRelayCapability = Uint8Array & {
  readonly __nativeRelayCapability: unique symbol;
};

export type NativeRelayExecutor = {
  execute(commandBase64: string): Promise<string>;
};

export type NativeRelayCommand =
  | { type: "open"; capability: NativeRelayCapability }
  | { type: "attach"; relay: bigint }
  | { type: "close-client"; client: bigint }
  | { type: "close-relay"; relay: bigint }
  | { type: "pump"; relay: bigint }
  | { type: "send-client-frame"; client: bigint; frame: Uint8Array }
  | { type: "receive-client-frames"; client: bigint };

type Response =
  | { type: "opened"; relay: bigint }
  | { type: "attached"; client: bigint }
  | { type: "closed"; closed: boolean }
  | { type: "pumped" }
  | { type: "frames"; frames: Uint8Array[] };

const ABI_MINIMUM = 3;
const ABI_MAXIMUM = 3;

/**
 * Encodes the small, versioned relay command ABI. It is deliberately limited
 * to lifecycle and canonical peer frames: no SQLite, query, or row operation
 * can cross this boundary.
 */
export function encodeNativeRelayCommand(command: NativeRelayCommand): string {
  const writer = new RelayPostcardWriter();
  switch (command.type) {
    case "open":
      assertCapability(command.capability);
      writer.u64(1).u64(ABI_MINIMUM).u64(ABI_MAXIMUM).raw(command.capability);
      break;
    case "attach":
      writer.u64(2).u64(command.relay);
      break;
    case "close-client":
      writer.u64(3).u64(command.client);
      break;
    case "close-relay":
      writer.u64(4).u64(command.relay);
      break;
    case "pump":
      writer.u64(5).u64(command.relay);
      break;
    case "send-client-frame":
      writer.u64(6).u64(command.client).bytes(command.frame);
      break;
    case "receive-client-frames":
      writer.u64(7).u64(command.client);
      break;
  }
  return base64Encode(writer.finish());
}

export function decodeNativeRelayResponse(encoded: string): Response {
  const reader = new RelayPostcardReader(base64Decode(encoded));
  const tag = Number(reader.u64());
  const response =
    tag === 1
      ? { type: "opened" as const, relay: reader.u64() }
      : tag === 2
        ? { type: "attached" as const, client: reader.u64() }
        : tag === 3
          ? { type: "closed" as const, closed: reader.bool() }
          : tag === 4
            ? { type: "pumped" as const }
            : tag === 5
              ? {
                  type: "frames" as const,
                  frames: reader.vec(() => reader.bytes()),
                }
              : (() => {
                  throw new Error(`Jazz native relay returned unknown response tag ${tag}`);
                })();
  if (!reader.done()) throw new Error("Jazz native relay response has trailing bytes");
  return response;
}

export class ReactNativeRelayFrameAdapter {
  private relay: bigint | null = null;
  private client: bigint | null = null;
  private closed = false;
  private scheduled = false;
  private running: Promise<void> = Promise.resolve();
  private readonly pendingFrames: Uint8Array[] = [];
  private readonly removeWorkListener: () => void;

  constructor(
    private readonly runtime: PeerTransportRuntime,
    private readonly transport: Transport,
    private readonly executor: NativeRelayExecutor,
    private readonly capability: NativeRelayCapability,
    private readonly onError: (error: Error) => void,
  ) {
    this.removeWorkListener = runtime.onPeerTransportWork(() => this.schedule());
  }

  async start(): Promise<void> {
    const opened = await this.execute({
      type: "open",
      capability: this.capability,
    });
    if (opened.type !== "opened")
      throw new Error("Jazz native relay did not open an admitted scope");
    this.relay = opened.relay;
    const attached = await this.execute({
      type: "attach",
      relay: opened.relay,
    }).catch(async (error) => {
      await this.execute({ type: "close-relay", relay: opened.relay }).catch(() => undefined);
      throw error;
    });
    if (attached.type !== "attached") throw new Error("Jazz native relay did not attach a UI peer");
    this.client = attached.client;
    this.transport.setOutboundScheduler?.(() => this.schedule());
    this.schedule();
  }

  /** Queue peer work; execution is serialized to preserve canonical frame order. */
  schedule(): void {
    if (this.closed || this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      this.running = this.running
        .then(() => this.progress())
        .catch((error: unknown) => {
          if (!this.closed) this.onError(asError(error));
        });
    });
  }

  receive(frames: readonly Uint8Array[]): void {
    if (this.closed || frames.length === 0) return;
    this.pendingFrames.push(...frames.map((frame) => frame.slice()));
    this.schedule();
  }

  async flush(): Promise<void> {
    this.schedule();
    // `schedule` installs the chain in a microtask so a synchronous caller
    // cannot observe the previous completed promise as a false flush.
    do {
      await Promise.resolve();
      await this.running;
    } while (this.scheduled);
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.removeWorkListener();
    this.transport.clearOutboundScheduler?.();
    await this.running;
    const client = this.client;
    this.client = null;
    const relay = this.relay;
    this.relay = null;
    if (client !== null)
      await this.execute({ type: "close-client", client }).catch(() => undefined);
    if (relay !== null) await this.execute({ type: "close-relay", relay }).catch(() => undefined);
    await this.runtime.retirePeerTransport(this.transport);
  }

  private async progress(): Promise<void> {
    if (this.closed || this.client === null || this.relay === null) return;
    const outbound = this.transport.recvWireFrames().map(normalizeFrame);
    if (outbound.length > 0) this.pendingFrames.push(...outbound);
    while (this.pendingFrames.length > 0) {
      const frame = this.pendingFrames[0]!;
      await this.expectPumped({
        type: "send-client-frame",
        client: this.client,
        frame,
      });
      this.pendingFrames.shift();
    }
    await this.expectPumped({ type: "pump", relay: this.relay });
    const response = await this.execute({
      type: "receive-client-frames",
      client: this.client,
    });
    if (response.type !== "frames") throw new Error("Jazz native relay did not return peer frames");
    if (response.frames.length > 0) {
      if (this.transport.sendWireFrames) this.transport.sendWireFrames(response.frames);
      else for (const frame of response.frames) this.transport.sendWireFrame(frame);
      this.runtime.notifyPeerTransportActivity?.();
    }
    await this.runtime.progressPeerTransport();
  }

  private async expectPumped(command: NativeRelayCommand): Promise<void> {
    const response = await this.execute(command);
    if (response.type !== "pumped")
      throw new Error("Jazz native relay rejected peer transport work");
  }

  private async execute(command: NativeRelayCommand): Promise<Response> {
    return decodeNativeRelayResponse(
      await this.executor.execute(encodeNativeRelayCommand(command)),
    );
  }
}

function assertCapability(value: Uint8Array): void {
  if (value.byteLength !== 32)
    throw new Error("Jazz native relay admission capability must be exactly 32 opaque bytes");
}
function normalizeFrame(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value))
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw new Error("Jazz React Native relay received a non-binary peer frame");
}
function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

class RelayPostcardWriter {
  private readonly parts: number[] = [];
  u64(value: number | bigint): this {
    let current = BigInt(value);
    if (current < 0n) throw new Error("postcard unsigned integer is negative");
    while (current >= 0x80n) {
      this.parts.push(Number((current & 0x7fn) | 0x80n));
      current >>= 7n;
    }
    this.parts.push(Number(current));
    return this;
  }
  raw(value: Uint8Array): this {
    this.parts.push(...value);
    return this;
  }
  bytes(value: Uint8Array): this {
    return this.u64(value.byteLength).raw(value);
  }
  finish(): Uint8Array {
    return Uint8Array.from(this.parts);
  }
}
class RelayPostcardReader {
  private offset = 0;
  constructor(private readonly value: Uint8Array) {}
  u64(): bigint {
    let result = 0n;
    let shift = 0n;
    for (;;) {
      if (this.offset >= this.value.length || shift >= 64n)
        throw new Error("invalid native relay postcard integer");
      const byte = this.value[this.offset++];
      result |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return result;
      shift += 7n;
    }
  }
  bool(): boolean {
    const value = this.byte();
    if (value === 0) return false;
    if (value === 1) return true;
    throw new Error("invalid native relay bool");
  }
  bytes(): Uint8Array {
    const length = Number(this.u64());
    const end = this.offset + length;
    if (!Number.isSafeInteger(length) || end > this.value.length)
      throw new Error("invalid native relay bytes");
    const result = this.value.slice(this.offset, end);
    this.offset = end;
    return result;
  }
  vec<T>(read: () => T): T[] {
    const length = Number(this.u64());
    if (!Number.isSafeInteger(length) || length > this.value.length - this.offset)
      throw new Error("invalid native relay vector");
    return Array.from({ length }, read);
  }
  done(): boolean {
    return this.offset === this.value.length;
  }
  private byte(): number {
    if (this.offset >= this.value.length)
      throw new Error("unexpected end of native relay response");
    return this.value[this.offset++];
  }
}
function base64Encode(bytes: Uint8Array): string {
  if (!globalThis.btoa) throw new Error("React Native relay requires a base64 encoder");
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return globalThis.btoa(binary);
}
function base64Decode(value: string): Uint8Array {
  if (!globalThis.atob) throw new Error("React Native relay requires a base64 decoder");
  return Uint8Array.from(globalThis.atob(value), (char) => char.charCodeAt(0));
}
