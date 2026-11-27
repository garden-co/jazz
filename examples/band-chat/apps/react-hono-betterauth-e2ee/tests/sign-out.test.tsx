// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderHook } from "@testing-library/react";
import { clearLeaveRecord, readLeaveRecord, writeLeaveRecord } from "../src/lib/leave-record";

const mocks = vi.hoisted(() => ({
  signOut: vi.fn(async () => ({ data: { success: true }, error: null }) as unknown),
  refresh: vi.fn(async () => {}),
  clearPendingMarkerIds: vi.fn(),
  redirect: vi.fn(),
}));

vi.mock("jazz-tools/react", () => ({ useJazzSession: () => ({ refresh: mocks.refresh }) }));
vi.mock("../src/lib/auth-client", () => ({ authClient: { signOut: mocks.signOut } }));
vi.mock("../src/lib/mark-read", () => ({ clearPendingMarkerIds: mocks.clearPendingMarkerIds }));

const { useBandChatLifecycle } = await import("../components/jazz-provider");

beforeEach(() => {
  localStorage.clear();
  writeLeaveRecord({ accountId: "account-alice", deviceId: "device-this" });
  mocks.signOut.mockReset();
  mocks.signOut.mockResolvedValue({ data: { success: true }, error: null } as never);
  mocks.refresh.mockClear();
  mocks.clearPendingMarkerIds.mockClear();
  mocks.redirect.mockClear();
});

afterEach(() => vi.unstubAllGlobals());

describe("BandChat sign-out", () => {
  it("keeps local state when Better Auth returns an error", async () => {
    mocks.signOut.mockResolvedValueOnce({
      data: null,
      error: { message: "network down" },
    } as never);
    const { result } = renderHook(() => useBandChatLifecycle());

    await expect(result.current.signOut()).rejects.toThrow("network down");

    expect(mocks.clearPendingMarkerIds).not.toHaveBeenCalled();
    expect(readLeaveRecord()).toEqual({ accountId: "account-alice", deviceId: "device-this" });
    expect(mocks.refresh).not.toHaveBeenCalled();
  });

  it("clears local state, refreshes Jazz and redirects after success", async () => {
    vi.stubGlobal("window", { location: { assign: mocks.redirect } });
    const { result } = renderHook(() => useBandChatLifecycle());

    await result.current.signOut();

    expect(mocks.clearPendingMarkerIds).toHaveBeenCalledOnce();
    expect(readLeaveRecord()).toBeNull();
    expect(mocks.refresh).toHaveBeenCalledOnce();
    expect(mocks.redirect).toHaveBeenCalledWith("/");
  });
});
