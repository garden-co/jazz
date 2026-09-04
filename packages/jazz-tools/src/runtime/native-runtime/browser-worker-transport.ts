import type { Transport } from "./native-runtime-adapter.js";

// Keep auxiliary chunk traffic below a bounded structured-clone allocation.
// A host may emit 256 KiB content-tree nodes, so this leaves room for several
// complete frames without giving one MessagePort task an unbounded payload.
export const MAX_AUXILIARY_FRAMES_PER_PORT_MESSAGE = 8;
export const MAX_AUXILIARY_BYTES_PER_PORT_MESSAGE = 1024 * 1024;

/** Redacted, bounded relay diagnostics from one WASM transport hop. */
export type AuxiliaryRelayTrace = Readonly<{
  event: string;
  role: "upstream" | "subscriber";
  connection: string;
  requestId: string;
  remainingHops: number;
  objectHash: string;
  locatorFingerprint: string;
  response?: "found" | "unavailable" | "retryable";
  storageError?: "unavailable" | "locator-conflict" | "integrity" | "backend";
}>;

export interface PeerTransportRuntime {
  onPeerTransportWork(listener: (requiresDistinctPass?: boolean) => void): () => void;
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
  private readonly handleRuntimeWork = (requiresDistinctPass?: boolean) =>
    this.schedule(requiresDistinctPass ?? false);
  constructor(
    private readonly runtime: PeerTransportRuntime,
    private readonly transport: Transport,
    private readonly sendFrames: (frames: Uint8Array[]) => void,
    private readonly onError: (error: unknown) => void,
    private readonly onAuxiliaryTrace?: (entries: AuxiliaryRelayTrace[]) => void,
    private readonly onPumpTrace?: (phase: "scheduled" | "drained", frameCount: number) => void,
  ) {
    // The evaluator notifies every peer after a pass. This pump drains its
    // transport immediately after the pass it requested, so that notification
    // must not recursively request another identical pass.
    this.removeWorkListener = runtime.onPeerTransportWork(this.handleRuntimeWork);
    this.transport.setAuxiliaryTraceEnabled?.(onAuxiliaryTrace !== undefined);
    this.transport.setOutboundScheduler?.(() => this.scheduleOutboundDrain());
    void this.watchAuxiliaryOutbound().catch((error) => {
      if (!this.closed) this.onError(error);
    });
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
    this.transport.setAuxiliaryTraceEnabled?.(false);
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

  drainOutboundFrames(): boolean {
    if (this.closed) return false;
    const frames = normalizeTransportFrames(this.transport.recvWireFrames());
    this.onPumpTrace?.("drained", frames.length);
    if (frames.length > 0) this.sendFrames(frames);
    const auxiliary = this.transport.recvAuxiliaryWireFrames;
    if (!auxiliary) return false;
    const auxiliaryFrames = normalizeTransportFrames(
      auxiliary.call(
        this.transport,
        MAX_AUXILIARY_FRAMES_PER_PORT_MESSAGE,
        MAX_AUXILIARY_BYTES_PER_PORT_MESSAGE,
      ),
    );
    if (auxiliaryFrames.length > 0) this.sendFrames(auxiliaryFrames);
    this.publishAuxiliaryTrace();
    return auxiliaryFrames.length > 0;
  }

  private scheduleOutboundDrain(): void {
    if (this.closed || this.outboundDrainScheduled) return;
    this.outboundDrainScheduled = true;
    this.onPumpTrace?.("scheduled", 0);
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
    this.publishAuxiliaryTrace();
    this.schedule();
  }

  private publishAuxiliaryTrace(): void {
    if (!this.onAuxiliaryTrace) return;
    const entries = this.transport.takeAuxiliaryTrace?.();
    if (!entries || entries.length === 0) return;
    this.onAuxiliaryTrace(entries as AuxiliaryRelayTrace[]);
  }

  private async watchAuxiliaryOutbound(): Promise<void> {
    while (!this.closed) {
      const readiness = this.transport.auxiliaryOutboundReady?.();
      if (!readiness || typeof readiness === "boolean") return;
      await readiness;
      if (this.closed) return;
      // This path is deliberately independent of progressPeerTransport(): a
      // root evaluator may be suspended on this very chunk request.
      const drainedAuxiliary = this.drainOutboundFrames();
      // `auxiliaryOutboundReady()` remains immediately ready while a bounded
      // drain leaves a remainder. Yield a browser task between batches so an
      // upload burst cannot spin in microtasks or starve MessagePort input.
      if (drainedAuxiliary) await yieldToBrowserTask();
    }
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

function yieldToBrowserTask(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}
