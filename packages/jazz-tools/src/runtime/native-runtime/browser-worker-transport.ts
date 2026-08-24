import type { Transport } from "./native-runtime-adapter.js";

export interface PeerTransportRuntime {
  onPeerTransportWork(listener: () => void): () => void;
  notifyPeerTransportActivity?(): void;
  progressPeerTransport(): Promise<void>;
  retirePeerTransport(transport: Transport): Promise<void>;
}

export class BrowserWorkerTransportPump {
  private scheduled = false;
  private running = false;
  private runAgain = false;
  private closed = false;
  private outboundDrainScheduled = false;
  // A MessagePort can deliver several batches before a WASM auxiliary routing
  // promise settles. Keep their wire order: a later semantic frame must not
  // overtake an earlier chunk response that wakes a suspended evaluator.
  private inboundRouting: Promise<void> = Promise.resolve();
  private requestedGeneration = 0;
  private completedGeneration = 0;
  private readonly flushWaiters = new Set<{ target: number; resolve: () => void }>();
  private readonly removeWorkListener: () => void;
  constructor(
    private readonly runtime: PeerTransportRuntime,
    private readonly transport: Transport,
    private readonly sendFrames: (frames: Uint8Array[]) => void,
    private readonly onError: (error: unknown) => void,
  ) {
    // The evaluator notifies every peer after a pass. This pump drains its
    // transport immediately after the pass it requested, so that notification
    // must not recursively request another identical pass.
    this.removeWorkListener = runtime.onPeerTransportWork(() => this.schedule(false));
    this.transport.setOutboundScheduler?.(() => this.scheduleOutboundDrain());
    this.schedule(true);
  }

  receive(frames: readonly Uint8Array[]): void {
    if (this.closed || frames.length === 0) return;
    const operation = this.inboundRouting.then(() => this.routeInboundFrames(frames));
    this.inboundRouting = operation.catch((error) => {
      if (!this.closed) this.onError(error);
    });
  }

  schedule(runAgainIfRunning = true): void {
    if (this.closed) return;
    this.requestedGeneration += 1;
    if (this.running) {
      this.runAgain ||= runAgainIfRunning;
      return;
    }
    if (this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      void this.pump().catch((error) => {
        if (!this.closed) this.onError(error);
      });
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.removeWorkListener();
    this.transport.clearOutboundScheduler?.();
    void this.runtime.retirePeerTransport(this.transport).catch(this.onError);
    for (const waiter of this.flushWaiters) waiter.resolve();
    this.flushWaiters.clear();
  }

  async flush(): Promise<void> {
    if (this.closed) return;
    this.schedule();
    const target = this.requestedGeneration;
    if (this.completedGeneration >= target) return;
    await new Promise<void>((resolve) => this.flushWaiters.add({ target, resolve }));
  }

  drainOutboundFrames(): void {
    if (this.closed) return;
    const frames = normalizeTransportFrames(this.transport.recvWireFrames());
    if (frames.length > 0) this.sendFrames(frames);
  }

  private scheduleOutboundDrain(): void {
    if (this.closed || this.outboundDrainScheduled) return;
    this.outboundDrainScheduled = true;
    queueMicrotask(() => {
      this.outboundDrainScheduled = false;
      if (!this.closed) this.drainOutboundFrames();
    });
  }

  private async pump(): Promise<void> {
    if (this.closed || this.running) return;
    this.running = true;
    const generation = this.requestedGeneration;
    try {
      this.drainOutboundFrames();
      await this.runtime.progressPeerTransport();
      if (this.closed) return;
      this.drainOutboundFrames();
    } finally {
      this.running = false;
      this.completedGeneration = Math.max(this.completedGeneration, generation);
      for (const waiter of this.flushWaiters) {
        if (waiter.target > this.completedGeneration) continue;
        this.flushWaiters.delete(waiter);
        waiter.resolve();
      }
    }
    if (this.closed) return;
    if (this.runAgain) {
      this.runAgain = false;
      this.schedule();
    }
  }

  private async routeInboundFrames(frames: readonly Uint8Array[]): Promise<void> {
    if (this.closed) return;
    const canonical: Uint8Array[] = [];
    for (const frame of frames) {
      const routed = this.transport.routeAuxiliaryWireFrame
        ? await this.transport.routeAuxiliaryWireFrame(frame)
        : frame;
      if (routed != null) canonical.push(normalizeTransportFrame(routed));
    }
    if (this.closed) return;
    if (canonical.length > 0) {
      if (this.transport.sendWireFrames) {
        this.transport.sendWireFrames(canonical);
      } else {
        for (const frame of canonical) this.transport.sendWireFrame(frame);
      }
    }
    // Coverage waiters need evidence of a peer response, not merely evidence
    // that our own pump ran after sending a request. Auxiliary frames count:
    // they can be the response that resumes a blocked query evaluation.
    this.runtime.notifyPeerTransportActivity?.();
    this.schedule();
  }
}

export function transferableFrames(frames: readonly Uint8Array[]): Uint8Array[] {
  return frames.map((frame) => frame.slice());
}

function normalizeTransportFrames(frames: unknown[]): Uint8Array[] {
  return frames.map(normalizeTransportFrame);
}

function normalizeTransportFrame(frame: unknown): Uint8Array {
  if (frame instanceof Uint8Array) return frame;
  if (frame instanceof ArrayBuffer) return new Uint8Array(frame);
  if (ArrayBuffer.isView(frame)) {
    return new Uint8Array(frame.buffer, frame.byteOffset, frame.byteLength);
  }
  throw new Error("Browser worker transport received a non-binary wire frame");
}
