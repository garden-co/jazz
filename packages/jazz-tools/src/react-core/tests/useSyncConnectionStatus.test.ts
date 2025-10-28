// @vitest-environment happy-dom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useSyncConnectionStatus } from "../hooks";
import {
  createJazzTestAccount,
  setupJazzTestSync,
  MockConnectionStatus,
} from "../testing";
import { act, cleanup, renderHook } from "./testUtils";

describe("useSyncConnectionStatus", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
    cleanup();
  });

  it("should return true by default in the test environment", () => {
    const { result } = renderHook(() => useSyncConnectionStatus());

    expect(result.current).toBe(true);
  });

  it("should handle updates", async () => {
    const { result } = renderHook(() => useSyncConnectionStatus());

    expect(result.current).toBe(true);

    act(() => {
      MockConnectionStatus.setIsConnected(false);
    });

    expect(result.current).toBe(false);

    act(() => {
      MockConnectionStatus.setIsConnected(true);
    });

    expect(result.current).toBe(true);
  });
});
