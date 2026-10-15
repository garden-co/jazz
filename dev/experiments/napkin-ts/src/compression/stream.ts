// Bounded streaming channel codecs. The browser build carries only the zstd
// decoder (Rust: ruzstd FrameDecoder; here: fzstd's streaming Decompress).
import { Decompress } from "fzstd";

export const MAX_STREAM_CHUNK_BYTES = 64 * 1024;
export const MAX_STREAM_PENDING_BYTES = MAX_STREAM_CHUNK_BYTES + 11;

export type Codec = "Lz4" | "Zstd";

export interface Progress {
  consumed: number;
  written: number;
  finished: boolean;
}
const noProgress = (): Progress => ({ consumed: 0, written: 0, finished: false });

const err = (e: unknown) =>
  new Error(`channel codec: ${e instanceof Error ? e.message : String(e)}`);

/** No encoder is compiled into the browser build. */
export class StreamEncoder {
  private failed = false;
  private ending = false;
  private ended = false;

  constructor(_codec: Codec) {
    throw new Error("channel encoder feature is not compiled in");
  }

  encode(input: Uint8Array, output: Uint8Array): Progress {
    if (this.failed || this.ending || this.ended) throw new Error("channel encoder is closed");
    if (output.length === 0) return noProgress();
    this.failed = true;
    throw new Error("channel encoder unavailable");
  }

  flush(output: Uint8Array): Progress {
    return this.flushOrFinish(output, false);
  }

  finish(output: Uint8Array): Progress {
    return this.flushOrFinish(output, true);
  }

  private flushOrFinish(output: Uint8Array, finish: boolean): Progress {
    if (this.failed || this.ended || (this.ending && !finish))
      throw new Error("channel encoder is closed");
    if (output.length === 0) return noProgress();
    this.failed = true;
    throw new Error("channel encoder unavailable");
  }
}

function drain(state: { pending: Uint8Array; position: number }, output: Uint8Array): Progress {
  const written = Math.min(output.length, state.pending.length - state.position);
  output.set(state.pending.subarray(state.position, state.position + written));
  state.position += written;
  if (state.position === state.pending.length) {
    state.pending = new Uint8Array(0);
    state.position = 0;
  }
  return { consumed: 0, written, finished: false };
}

export class StreamDecoder {
  private readonly zstd?: Decompress;
  private readonly out = { pending: new Uint8Array(0), position: 0 };
  private packet: number[] = [];
  private target: number;
  private packetPosition = 0;
  private blockLimit = MAX_STREAM_CHUNK_BYTES;
  private header = true;
  private packetReady = false;
  private lastBlock = false;
  private ended = false;
  private failed = false;

  constructor(private readonly codec: Codec) {
    if (codec !== "Zstd") throw new Error("channel decoder feature is not compiled in");
    this.zstd = new Decompress((chunk) => {
      const merged = new Uint8Array(this.out.pending.length - this.out.position + chunk.length);
      merged.set(this.out.pending.subarray(this.out.position));
      merged.set(chunk, merged.length - chunk.length);
      this.out.pending = merged;
      this.out.position = 0;
    });
    this.target = 6;
  }

  decode(input: Uint8Array, output: Uint8Array): Progress {
    if (this.failed) throw new Error("channel decoder is poisoned");
    try {
      return this.decodeInner(input, output);
    } catch (e) {
      this.failed = true;
      throw e;
    }
  }

  private decodeInner(input: Uint8Array, output: Uint8Array): Progress {
    if (this.ended) {
      if (input.length) throw new Error("bytes after channel codec end");
      return { consumed: 0, written: 0, finished: true };
    }
    if (output.length === 0) return noProgress();
    const progress = noProgress();
    for (;;) {
      if (!this.packetReady) {
        const amount = Math.min(this.target - this.packet.length, input.length - progress.consumed);
        for (let i = 0; i < amount; i++) this.packet.push(input[progress.consumed + i]);
        progress.consumed += amount;
        if (this.packet.length < this.target) return progress;
        if (this.header) {
          this.validateHeader();
          this.packetReady = true;
        } else {
          if (this.target === this.blockHeaderLen()) {
            const [bodyLen, last] = this.blockSize();
            this.lastBlock = last;
            this.target += bodyLen;
            if (bodyLen !== 0) continue;
          }
          this.packetReady = true;
        }
      }
      const [consumed, written, drained] = this.processPacket(output.subarray(progress.written));
      this.packetPosition += consumed;
      progress.written += written;
      if (drained) {
        const last = !this.header && this.lastBlock;
        this.header = false;
        this.packet = [];
        this.packetPosition = 0;
        this.packetReady = false;
        this.target = this.blockHeaderLen();
        if (last) {
          this.ended = true;
          progress.finished = true;
          return progress;
        }
      }
      if (progress.written === output.length) return progress;
      if (!drained && consumed === 0 && written === 0)
        throw new Error("channel decoder stalled on a complete block");
    }
  }

  finish(): void {
    if (this.failed || !this.ended) throw new Error("truncated or undrained channel codec stream");
  }

  private blockHeaderLen(): number {
    return this.codec === "Lz4" ? 4 : 3;
  }

  private validateHeader(): void {
    const p = this.packet;
    const valid =
      this.codec === "Lz4"
        ? [0x04, 0x22, 0x4d, 0x18, 0x40, 0x40, 0xc0].every((b, i) => p[i] === b)
        : p[0] === 0x28 &&
          p[1] === 0xb5 &&
          p[2] === 0x2f &&
          p[3] === 0xfd &&
          p[4] === 0 &&
          p[5] <= 0x30;
    if (!valid) throw new Error("unsupported channel codec header/window profile");
    if (this.codec === "Zstd") {
      const descriptor = p[5];
      const base = 1 << (10 + (descriptor >> 3));
      this.blockLimit = base + Math.floor(base / 8) * (descriptor & 7);
    }
  }

  private blockSize(): [number, boolean] {
    const p = this.packet;
    let size: number;
    let stored: number;
    let last: boolean;
    if (this.codec === "Lz4") {
      const value = (p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24)) >>> 0;
      size = value & 0x7fffffff;
      if (value !== 0 && size === 0) throw new Error("empty nonterminal lz4 block");
      [stored, last] = [size, value === 0];
    } else {
      const value = p[0] | (p[1] << 8) | (p[2] << 16);
      size = value >>> 3;
      const kind = (value >>> 1) & 3;
      if (kind === 3) throw new Error("reserved zstd block type");
      [stored, last] = [kind === 1 ? 1 : size, (value & 1) !== 0];
    }
    if (size > this.blockLimit) throw new Error("channel codec block exceeds 64KiB/profile window");
    return [stored, last];
  }

  private processPacket(output: Uint8Array): [number, number, boolean] {
    const input = Uint8Array.from(this.packet.slice(this.packetPosition));
    if (this.packetPosition === 0) {
      try {
        this.zstd!.push(input, !this.header && this.lastBlock);
      } catch (e) {
        throw err(e);
      }
      if (this.header && this.out.pending.length > this.blockLimit)
        throw err("block output exceeds window");
    }
    const { written } = drain(this.out, output);
    return [input.length, written, this.out.pending.length === 0];
  }
}
