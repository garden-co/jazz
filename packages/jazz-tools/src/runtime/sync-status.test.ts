import { describe, expect, it } from "vitest";
import {
  linkStatusFromUpstream,
  sameSyncStatus,
  syncStatus,
  type UpstreamLinkStatus,
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
    expect(syncStatus({ state: "connected" }, 0).synced).toBe(true);
    expect(syncStatus({ state: "local-only" }, 0).synced).toBe(true);
    expect(syncStatus({ state: "connected" }, 1).synced).toBe(false);
    expect(syncStatus({ state: "connecting" }, 0).synced).toBe(false);
    expect(
      syncStatus({ state: "offline", reason: "disconnected", retrying: false }, 0).synced,
    ).toBe(false);
  });

  it("compares by value", () => {
    const network = {
      state: "offline",
      reason: "network",
      retrying: true,
    } as const;
    expect(sameSyncStatus(syncStatus(network, 1), syncStatus({ ...network }, 1))).toBe(true);
    expect(sameSyncStatus(syncStatus(network, 1), syncStatus(network, 2))).toBe(false);
    expect(
      sameSyncStatus(
        syncStatus(network, 1),
        syncStatus({ state: "offline", reason: "auth", retrying: false }, 1),
      ),
    ).toBe(false);
  });
});
