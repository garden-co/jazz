// @vitest-environment jsdom
import React from "react";
import { describe, it, expect } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { JazzClientProvider } from "./provider.js";
import { useAuthState } from "./use-auth-state.js";
import { makeFakeClient } from "./test-utils.js";

describe("useAuthState", () => {
  it("returns authMode, userId, claims", async () => {
    const client = makeFakeClient({
      authMode: "local-first",
      userId: "u-1",
      claims: { role: "admin" },
    });
    const { result } = renderHook(() => useAuthState(), {
      wrapper: ({ children }) => (
        <JazzClientProvider client={client as any}>{children}</JazzClientProvider>
      ),
    });
    expect(result.current.authMode).toBe("local-first");
    expect(result.current.userId).toBe("u-1");
    expect(result.current.claims).toEqual({ role: "admin" });
    expect(result.current.error).toBeUndefined();
  });

  it("reflects markUnauthenticated via error field; preserves last-known userId", async () => {
    const client = makeFakeClient({ authMode: "external", userId: "u-1", claims: {} });
    const { result } = renderHook(() => useAuthState(), {
      wrapper: ({ children }) => (
        <JazzClientProvider client={client as any}>{children}</JazzClientProvider>
      ),
    });
    act(() => client.__markUnauthenticated("expired"));
    expect(result.current.error).toBe("expired");
    expect(result.current.userId).toBe("u-1");
  });

  it("returns no status or transport", () => {
    const client = makeFakeClient({ authMode: "external", userId: "u-1", claims: {} });
    const { result } = renderHook(() => useAuthState(), {
      wrapper: ({ children }) => (
        <JazzClientProvider client={client as any}>{children}</JazzClientProvider>
      ),
    });
    // @ts-expect-error — no status field
    result.current.status;
    // @ts-expect-error — no transport field
    result.current.transport;
  });
});
