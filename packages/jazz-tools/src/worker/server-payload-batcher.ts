/**
 * Accumulates server-bound sync payloads within a microtask boundary and
 * flushes them as a single ordered batch.
 *
 * This collapses N synchronous outbox callbacks (e.g. 60fps game loop writes)
 * into one sendSyncPayloadBatch call per tick, reducing HTTP round-trips
 * without losing or reordering any intermediate states.
 */
export class ServerPayloadBatcher {
  private pending: string[] = [];
  private flushQueued = false;

  constructor(private readonly sendBatch: (payloads: string[]) => Promise<void>) {}

  enqueue(payloadJson: string): void {
    this.pending.push(payloadJson);
    if (this.flushQueued) return;

    this.flushQueued = true;
    queueMicrotask(() => {
      this.flushQueued = false;
      const payloads = this.pending;
      this.pending = [];
      if (payloads.length === 0) return;
      this.sendBatch(payloads).catch((error) => {
        console.error("[ServerPayloadBatcher] sendBatch error:", error);
      });
    });
  }
}
