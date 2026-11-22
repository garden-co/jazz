import { describe, expect, it } from "vitest";
import {
  linkStatusFromUpstream,
  CONNECTED,
  latestSyncError,
  linkFailureOf,
  offlineLink,
  sameSyncStatus,
  samePendingUploadRows,
  sameUploadStatus,
  syncStatus,
  type UpstreamLinkStatus,
  uploadStatusFromCount,
} from "./sync-status.js";

const upstream = (
  phase: UpstreamLinkStatus["phase"],
  outagePublished = false,
): UpstreamLinkStatus => ({ phase, outagePublished });

describe("linkStatusFromUpstream", () => {
  it("maps every phase of the core link machine", () => {
    expect(linkStatusFromUpstream(null)).toEqual({ state: "local-only" });
    expect(linkStatusFromUpstream(upstream("noServer"))).toEqual({
      state: "local-only",
    });
    expect(linkStatusFromUpstream(upstream("live"))).toEqual({
      state: "connected",
    });
    expect(linkStatusFromUpstream(upstream("offline"))).toEqual({
      state: "offline",
      reason: "disconnected",
      retrying: false,
    });
    expect(linkStatusFromUpstream(upstream("awaitingCredential"))).toEqual({
      state: "offline",
      reason: "auth",
      retrying: false,
    });
    expect(linkStatusFromUpstream(upstream("stopped"))).toEqual({
      state: "offline",
      reason: "stopped",
      retrying: false,
    });
  });

  it("shows a retried drop as connecting until the outage is published", () => {
    expect(linkStatusFromUpstream(upstream("backoff"))).toEqual({
      state: "connecting",
    });
    expect(linkStatusFromUpstream(upstream("connecting"))).toEqual({
      state: "connecting",
    });
    const outage = { state: "offline", reason: "network", retrying: true };
    expect(linkStatusFromUpstream(upstream("backoff", true))).toEqual(outage);
    expect(linkStatusFromUpstream(upstream("connecting", true))).toEqual(outage);
  });
});

describe("syncStatus", () => {
  it("is synced only with nothing pending on a connected or server-less link", () => {
    expect(syncStatus({ state: "connected" }, uploadStatusFromCount(0)).synced).toBe(true);
    expect(syncStatus({ state: "local-only" }, uploadStatusFromCount(0)).synced).toBe(true);
    expect(syncStatus({ state: "connected" }, uploadStatusFromCount(1)).synced).toBe(false);
    expect(syncStatus({ state: "connecting" }, uploadStatusFromCount(0)).synced).toBe(false);
    expect(
      syncStatus(
        { state: "offline", reason: "disconnected", retrying: false },
        uploadStatusFromCount(0),
      ).synced,
    ).toBe(false);
  });

  it("compares by value", () => {
    const network = {
      state: "offline",
      reason: "network",
      retrying: true,
    } as const;
    expect(
      sameSyncStatus(
        syncStatus(network, uploadStatusFromCount(1)),
        syncStatus({ ...network }, uploadStatusFromCount(1)),
      ),
    ).toBe(true);
    expect(
      sameSyncStatus(
        syncStatus(network, uploadStatusFromCount(1)),
        syncStatus(network, uploadStatusFromCount(2)),
      ),
    ).toBe(false);
    expect(
      sameSyncStatus(
        syncStatus(network, uploadStatusFromCount(1)),
        syncStatus({ state: "offline", reason: "auth", retrying: false }, uploadStatusFromCount(1)),
      ),
    ).toBe(false);
  });

  it("reports when the oldest pending write was made, and what the server rejected", () => {
    const uploads = {
      pending: 2,
      oldestPendingAt: 1_000,
      rejected: 3,
      lastRejection: { code: "permission_denied", message: "no", at: 2_000 },
      pendingRevision: 0,
    };
    const status = syncStatus(CONNECTED, uploads);
    expect(status).toMatchObject({
      pendingUploads: 2,
      oldestPendingAt: 1_000,
      rejected: 3,
      lastError: { kind: "rejected", code: "permission_denied", message: "no", at: 2_000 },
      synced: false,
    });
    // Nothing pending has no oldest pending write, whatever the core said.
    expect(syncStatus(CONNECTED, { ...uploads, pending: 0 }).oldestPendingAt).toBeNull();
    expect(syncStatus(CONNECTED).lastError).toBeNull();
  });

  it("names the later of a rejection and a link failure as the last error", () => {
    const rejection = { code: "permission_denied", message: "no", at: 2_000 };
    const uploads = { ...uploadStatusFromCount(0), rejected: 1, lastRejection: rejection };
    const earlier = { kind: "network", message: "down", at: 1_000 } as const;
    const later = { kind: "auth", message: "refused", at: 3_000 } as const;
    expect(latestSyncError(uploads, earlier)).toEqual({ kind: "rejected", ...rejection });
    expect(latestSyncError(uploads, later)).toEqual(later);
    expect(latestSyncError(uploadStatusFromCount(0), earlier)).toEqual(earlier);
    expect(latestSyncError(uploadStatusFromCount(0), null)).toBeNull();
    expect(
      sameSyncStatus(
        syncStatus(CONNECTED, uploads, earlier),
        syncStatus(CONNECTED, uploads, later),
      ),
    ).toBe(false);
  });

  it("takes a failing link as an error, but not one the app disconnected", () => {
    expect(linkFailureOf(offlineLink("network"), "socket closed")).toMatchObject({
      kind: "network",
      message: "socket closed",
    });
    expect(linkFailureOf(offlineLink("auth"), undefined)?.message).toMatch(/refused/);
    expect(linkFailureOf(offlineLink("stopped"), undefined)?.kind).toBe("stopped");
    expect(linkFailureOf(offlineLink("disconnected"), "x")).toBeNull();
    expect(linkFailureOf(CONNECTED, "x")).toBeNull();
  });
});

describe("pending upload revision", () => {
  it("tells apart two statuses that differ only in which writes are pending", () => {
    // One write settled as another joined: same count, same oldest time.
    const before = { ...uploadStatusFromCount(1), oldestPendingAt: 5, pendingRevision: 1 };
    const after = { ...before, pendingRevision: 3 };
    expect(sameUploadStatus(before, after)).toBe(false);
    expect(sameUploadStatus(after, { ...after })).toBe(true);
  });

  it("compares pending rows by content, and unknown only with unknown", () => {
    expect(samePendingUploadRows(["a", "b"], ["a", "b"])).toBe(true);
    expect(samePendingUploadRows(["a"], ["b"])).toBe(false);
    expect(samePendingUploadRows([], null)).toBe(false);
    expect(samePendingUploadRows(null, null)).toBe(true);
  });
});
