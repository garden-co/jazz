// @vitest-environment jsdom
import React from "react";
import { describe, it, expect } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { JazzClientProvider } from "./provider.js";
import { useSyncStatus } from "./use-sync-status.js";
import { makeFakeClient } from "./test-utils.js";

describe("useSyncStatus", () => {
  it("renders the Db's status and follows its changes", () => {
    const client = makeFakeClient({
      authMode: "local-first",
      userId: "u-1",
      claims: {},
    });
    const { result, unmount } = renderHook(() => useSyncStatus(), {
      wrapper: ({ children }) => (
        <JazzClientProvider client={client as any}>{children}</JazzClientProvider>
      ),
    });
    expect(result.current).toEqual({
      link: { state: "connecting" },
      pendingUploads: 0,
      synced: false,
    });

    const offline = {
      link: { state: "offline", reason: "network", retrying: true },
      pendingUploads: 2,
      synced: false,
    } as const;
    act(() => client.__setSyncStatus(offline));
    expect(result.current).toBe(offline);

    unmount();
    expect(client.__syncListenerCount()).toBe(0);
  });
});
