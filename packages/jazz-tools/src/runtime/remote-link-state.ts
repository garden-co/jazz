/**
 * Live reachability of the upstream server, as observed by one host.
 *
 * - `none`: no server is configured, so no remote can hold other data.
 * - `connecting`: a server is configured and its first connection attempt
 *   (or an explicit reconnect) has not settled yet.
 * - `connected`: the upstream transport is admitted.
 * - `unavailable`: the application disconnected explicitly, the connection
 *   dropped and is retrying, or the attempt failed.
 *
 * Hosts report it to the core Db, which uses it only to decide whether an
 * empty local-first-unless-empty opening may wait for the server. It never
 * changes write durability or strict `ReadTier.Remote` reads.
 */
export type RemoteLinkState = "none" | "connecting" | "connected" | "unavailable";

/**
 * Small listener set that publishes a derived state only when it changed.
 * Publication is deferred to a microtask so that intermediate states inside
 * one synchronous transport transition are never observed.
 */
export class RemoteLinkStatePublisher {
  private readonly listeners = new Set<(state: RemoteLinkState) => void>();
  private published: RemoteLinkState = "none";
  private scheduled = false;

  constructor(private readonly read: () => RemoteLinkState) {}

  subscribe(listener: (state: RemoteLinkState) => void, signal: AbortSignal): void {
    if (signal.aborted) return;
    // Changes are not tracked while nobody listens; resynchronize first.
    if (this.listeners.size === 0) this.published = this.read();
    this.listeners.add(listener);
    signal.addEventListener("abort", () => this.listeners.delete(listener), {
      once: true,
    });
  }

  get hasListeners(): boolean {
    return this.listeners.size > 0;
  }

  /** Schedule a comparison against the last published state. */
  changed(): void {
    if (this.scheduled || this.listeners.size === 0) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      const state = this.read();
      if (state === this.published) return;
      this.published = state;
      for (const listener of [...this.listeners]) listener(state);
    });
  }
}
