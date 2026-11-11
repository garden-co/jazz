/** Why a {@link LinkStatus} is `offline`. */
export type OfflineReason =
  | "disconnected"
  | "network"
  | "auth"
  | "stopped"
  | "authority-changed"
  | "held-by-another-tab";

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
 *     one (`db.updateAuthToken(...)`, or a fresh sign-in through the app's
 *     backend).
 *   - `stopped`: a failure retrying cannot fix (a protocol error or an
 *     incompatible server); only `db.reconnect()` tries again.
 *   - `authority-changed`: the server serves another history than this
 *     client's local store (its store was recreated, or it is another app's
 *     server), so nothing syncs with it. Waits for the server fail with an
 *     `AuthorityChangedError`, which says how to recover: reset the local
 *     store (`db.deleteClientStorage()` in the browser). Nothing is reset
 *     automatically, so unsynced writes are never discarded behind the
 *     app's back.
 *   - `held-by-another-tab` (browser): another tab holds this account's
 *     local store under another sign-in (it was still the guest when this
 *     tab signed up) and has not let go for a while. The tab keeps trying
 *     (`retrying: true`) and joins once the other tab follows or closes;
 *     waits fail meanwhile with a `StoreHeldByAnotherTabError`.
 */
export type LinkStatus =
  | { state: "local-only" }
  | { state: "connecting" }
  | { state: "connected" }
  | {
      state: "offline";
      reason: OfflineReason;
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
   * When the oldest of {@link pendingUploads} was committed, in Unix ms, or
   * null when none waits. How long a write has been stuck is
   * `Date.now() - oldestPendingAt`.
   */
  oldestPendingAt: number | null;
  /**
   * This client's transactions the server rejected since the Db opened.
   * Each is also reported through `db.onMutationError`, or to the write
   * whose `wait()` claimed it.
   */
  rejected: number;
  /**
   * The latest thing that went wrong with sync since the Db opened, and
   * when: a rejected transaction, or the link failing. It stays after the
   * link recovers (`link` says how things are now), so an agent or a
   * debugging person can see what happened and when.
   */
  lastError: SyncError | null;
  /**
   * Nothing waits for upload and the link is connected, or no server is
   * configured.
   */
  synced: boolean;
}

/** What went wrong, as {@link SyncStatus.lastError} reports it. */
export type SyncError =
  | {
      /** The server rejected a transaction this client committed. */
      kind: "rejected";
      /** The public error code, such as `permission_denied`. */
      code: string;
      message: string;
      /** When the rejection arrived, in Unix ms. */
      at: number;
    }
  | LinkFailure;

/** The link failing, as {@link SyncError} reports it. */
export interface LinkFailure {
  /** Why, as an offline {@link LinkStatus} names it. */
  kind: Exclude<OfflineReason, "disconnected">;
  message: string;
  /** When the link failed this way, in Unix ms. */
  at: number;
}

/** @internal A client's uploads, as the core reports them. */
export interface UploadStatus {
  pending: number;
  oldestPendingAt: number | null;
  rejected: number;
  lastRejection: { code: string; message: string; at: number } | null;
}

/** @internal The upload status of a client that has uploaded nothing. */
export const NO_UPLOADS: UploadStatus = Object.freeze({
  pending: 0,
  oldestPendingAt: null,
  rejected: 0,
  lastRejection: null,
});

/** @internal The upload status from a count alone (a host without the full status). */
export function uploadStatusFromCount(pending: number): UploadStatus {
  return { ...NO_UPLOADS, pending };
}

/** @internal The later of a rejection and a link failure. */
export function latestSyncError(uploads: UploadStatus, link: LinkFailure | null): SyncError | null {
  const rejection = uploads.lastRejection
    ? ({ kind: "rejected", ...uploads.lastRejection } as const)
    : null;
  if (!rejection) return link;
  if (!link) return rejection;
  return link.at > rejection.at ? link : rejection;
}

/** @internal The failure an offline link reports, or null for a link that is not failing. */
export function linkFailureOf(link: LinkStatus, message: string | undefined): LinkFailure | null {
  if (link.state !== "offline" || link.reason === "disconnected") return null;
  const said = {
    network: "The server is unreachable; retrying.",
    auth: "The server refused this client's credential.",
    stopped: "The link stopped for good; only db.reconnect() tries again.",
    "authority-changed": "The server serves another history than this client's local store.",
    "held-by-another-tab": "Another tab holds this account's local store under another sign-in.",
  }[link.reason];
  return { kind: link.reason, message: message || said, at: Date.now() };
}

export function sameSyncError(a: SyncError | null, b: SyncError | null): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return (
    a.kind === b.kind &&
    a.at === b.at &&
    a.message === b.message &&
    (a.kind !== "rejected" || (b.kind === "rejected" && a.code === b.code))
  );
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
export function offlineLink(reason: OfflineReason, retrying = reason === "network"): LinkStatus {
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
export function syncStatus(
  link: LinkStatus,
  uploads: UploadStatus = NO_UPLOADS,
  linkFailure: LinkFailure | null = null,
): SyncStatus {
  return {
    link,
    pendingUploads: uploads.pending,
    oldestPendingAt: uploads.pending === 0 ? null : uploads.oldestPendingAt,
    rejected: uploads.rejected,
    lastError: latestSyncError(uploads, linkFailure),
    synced: uploads.pending === 0 && (link.state === "connected" || link.state === "local-only"),
  };
}

export function sameSyncStatus(a: SyncStatus, b: SyncStatus): boolean {
  return (
    a.pendingUploads === b.pendingUploads &&
    a.oldestPendingAt === b.oldestPendingAt &&
    a.rejected === b.rejected &&
    sameSyncError(a.lastError, b.lastError) &&
    sameLinkStatus(a.link, b.link)
  );
}

/** @internal Whether two upload statuses report the same. */
export function sameUploadStatus(a: UploadStatus, b: UploadStatus): boolean {
  return (
    a.pending === b.pending &&
    a.oldestPendingAt === b.oldestPendingAt &&
    a.rejected === b.rejected &&
    a.lastRejection?.at === b.lastRejection?.at &&
    a.lastRejection?.code === b.lastRejection?.code
  );
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
