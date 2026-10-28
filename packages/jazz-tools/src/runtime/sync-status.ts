/**
 * The upstream connection as an app should show it.
 *
 * - `local-only`: no server is configured; nothing is ever uploaded.
 * - `connecting`: the first connection attempt, or a reconnect after a short
 *   drop that has not been reported as an outage yet.
 * - `connected`: the server admitted this client.
 * - `offline`: not connected, with the reason:
 *   - `disconnected`: the app called `db.disconnect()`.
 *   - `network`: the server has been unreachable for a while; the client
 *     keeps retrying (`retrying: true`).
 *   - `auth`: the server refused the credential. The client waits for a new
 *     one (`db.updateAuthToken(...)` or the auth provider's refresh).
 *   - `stopped`: a failure retrying cannot fix (a protocol error or an
 *     incompatible server); only `db.reconnect()` tries again.
 */
export type LinkStatus =
  | { state: "local-only" }
  | { state: "connecting" }
  | { state: "connected" }
  | {
      state: "offline";
      reason: "disconnected" | "network" | "auth" | "stopped";
      retrying: boolean;
    };

/** Whether this client's data has reached the server, as `db.getSyncStatus()` reports it. */
export interface SyncStatus {
  /** The upstream connection. */
  link: LinkStatus;
  /**
   * Transactions this client committed that the server has neither accepted
   * nor rejected yet. In Node and React Native this includes ones recovered
   * from local storage after a restart; a browser tab counts only its own
   * commits, since the shared worker uploads recovered ones. Rejections are
   * reported through `db.onMutationError`.
   */
  pendingUploads: number;
  /**
   * Nothing waits for upload and the link is connected, or no server is
   * configured.
   */
  synced: boolean;
}

/** @internal The upstream link machine's status, as the core reports it to its host. */
export interface UpstreamLinkStatus {
  phase:
    | "noServer"
    | "offline"
    | "connecting"
    | "live"
    | "backoff"
    | "awaitingCredential"
    | "stopped";
  outagePublished: boolean;
}

export const LOCAL_ONLY: LinkStatus = Object.freeze({ state: "local-only" });
export const CONNECTING: LinkStatus = Object.freeze({ state: "connecting" });
export const CONNECTED: LinkStatus = Object.freeze({ state: "connected" });

/** @internal The app-facing status of an offline link. */
export function offlineLink(
  reason: "disconnected" | "network" | "auth" | "stopped",
  retrying = reason === "network",
): LinkStatus {
  return { state: "offline", reason, retrying };
}

/**
 * @internal Map the core link machine's status to {@link LinkStatus}. A
 * dropped link that is retrying reads as `connecting` until the machine
 * publishes an outage, so a short blip never shows as offline.
 */
export function linkStatusFromUpstream(status: UpstreamLinkStatus | null): LinkStatus {
  switch (status?.phase ?? "noServer") {
    case "noServer":
      return LOCAL_ONLY;
    case "offline":
      return offlineLink("disconnected");
    case "live":
      return CONNECTED;
    case "connecting":
    case "backoff":
      return status!.outagePublished ? offlineLink("network") : CONNECTING;
    case "awaitingCredential":
      return offlineLink("auth");
    case "stopped":
      return offlineLink("stopped");
  }
}

export function sameLinkStatus(a: LinkStatus, b: LinkStatus): boolean {
  if (a.state !== b.state) return false;
  if (a.state !== "offline" || b.state !== "offline") return true;
  return a.reason === b.reason && a.retrying === b.retrying;
}

/** @internal Assemble a {@link SyncStatus}. */
export function syncStatus(link: LinkStatus, pendingUploads: number): SyncStatus {
  return {
    link,
    pendingUploads,
    synced: pendingUploads === 0 && (link.state === "connected" || link.state === "local-only"),
  };
}

export function sameSyncStatus(a: SyncStatus, b: SyncStatus): boolean {
  return a.pendingUploads === b.pendingUploads && sameLinkStatus(a.link, b.link);
}

/**
 * @internal Listener set that publishes a derived value only when it changed.
 * Publication is deferred to a microtask so that intermediate values inside
 * one synchronous transition are never observed.
 */
export class DerivedStatePublisher<T> {
  private readonly listeners = new Set<(value: T) => void>();
  private published: T | undefined;
  private scheduled = false;

  constructor(
    private readonly read: () => T,
    private readonly same: (a: T, b: T) => boolean = Object.is,
  ) {}

  subscribe(listener: (value: T) => void, signal: AbortSignal): void {
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

  /** Schedule a comparison against the last published value. */
  changed(): void {
    if (this.scheduled || this.listeners.size === 0) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      const value = this.read();
      if (this.published !== undefined && this.same(value, this.published)) return;
      this.published = value;
      for (const listener of [...this.listeners]) listener(value);
    });
  }
}
