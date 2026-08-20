import type { Transport } from "./native-runtime-adapter.js";

export interface PeerTransportRuntime {
  onPeerTransportWork(listener: () => void): () => void;
  notifyPeerTransportActivity?(): void;
}

export class BrowserWorkerTransportPump {
  private scheduled = false;
  private running = false;
  private runAgain = false;
  private closed = false;
  private readonly removeWorkListener: () => void;

  constructor(
    private readonly runtime: PeerTransportRuntime,
    private readonly transport: Transport,
    private readonly sendFrames: (frames: Uint8Array[]) => void,
  ) {
    this.removeWorkListener = runtime.onPeerTransportWork(() => this.schedule());
    this.schedule();
  }

  receive(frames: readonly Uint8Array[]): void {
    if (this.closed || frames.length === 0) return;
    if (this.transport.sendWireFrames) {
      this.transport.sendWireFrames(frames);
    } else {
      for (const frame of frames) this.transport.sendWireFrame(frame);
    }
    // Coverage waiters need evidence of a peer response, not merely evidence
    // that our own pump ran after sending a request.
    this.runtime.notifyPeerTransportActivity?.();
    this.schedule();
  }

  schedule(): void {
    if (this.closed) return;
    if (this.running) {
      this.runAgain = true;
      return;
    }
    if (this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      void this.pump();
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.removeWorkListener();
    this.transport.close();
  }

  private async pump(): Promise<void> {
    if (this.closed || this.running) return;
    this.running = true;
    let exhausted = true;
    try {
      for (let round = 0; round < 32; round += 1) {
        const work = await this.transport.tick();
        const frames = normalizeTransportFrames(this.transport.recvWireFrames());
        if (frames.length > 0) this.sendFrames(frames);
        if (work === 0 && frames.length === 0) {
          exhausted = false;
          break;
        }
      }
    } finally {
      this.running = false;
    }
    if (this.closed) return;
    if (this.runAgain) {
      this.runAgain = false;
      this.schedule();
    } else if (exhausted) {
      setTimeout(() => this.schedule(), 0);
    }
  }
}

export function transferableFrames(frames: readonly Uint8Array[]): Uint8Array[] {
  return frames.map((frame) => frame.slice());
}

function normalizeTransportFrames(frames: unknown[]): Uint8Array[] {
  return frames.map((frame) => {
    if (frame instanceof Uint8Array) return frame;
    if (frame instanceof ArrayBuffer) return new Uint8Array(frame);
    if (ArrayBuffer.isView(frame)) {
      return new Uint8Array(frame.buffer, frame.byteOffset, frame.byteLength);
    }
    throw new Error("Browser worker transport received a non-binary wire frame");
  });
}
