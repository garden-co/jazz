// @vitest-environment jsdom
import React from "react";
import { describe, it, expect, vi } from "vitest";
import { renderHook } from "@testing-library/react";
import { JazzClientProvider, useSession } from "./provider.js";
import { useJazzSession } from "./session.js";
import { makeFakeClient } from "./test-utils.js";
import { SessionContext } from "./session-context.js";

function clientWithSession(authMode: "external" | "device-account" | "anonymous") {
  const client = makeFakeClient({ authMode, userId: "u-1", claims: { role: "admin" } });
  return Object.assign(client, { session: client.db.getAuthState().session });
}

const wrapperFor =
  (client: unknown) =>
  ({ children }: { children: React.ReactNode }) => (
    <JazzClientProvider client={client as any}>{children}</JazzClientProvider>
  );

describe("useJazzSession under <JazzClientProvider client>", () => {
  it("is ready, with the user the client's permissions see", () => {
    const client = clientWithSession("external");
    const { result } = renderHook(() => useJazzSession(), { wrapper: wrapperFor(client) });
    expect(result.current.status).toBe("ready");
    expect(result.current.client).toBe(client);
    expect(result.current.user).toEqual({
      accountId: "00000000-0000-4000-8000-000000000002",
      kind: "backend",
      identity: { issuer: "https://issuer.example", subject: "u-1" },
      claims: { role: "admin" },
    });
  });

  it("names a device account by kind and has no user for an anonymous reader", () => {
    const device = renderHook(() => useJazzSession(), {
      wrapper: wrapperFor(clientWithSession("device-account")),
    });
    expect(device.result.current.user?.kind).toBe("device");
    const anonymous = renderHook(() => useJazzSession(), {
      wrapper: wrapperFor(clientWithSession("anonymous")),
    });
    expect(anonymous.result.current.user).toBeNull();
  });

  it("refuses session operations, naming why", async () => {
    const { result } = renderHook(() => useJazzSession(), {
      wrapper: wrapperFor(clientWithSession("external")),
    });
    await expect(result.current.signOut()).rejects.toThrow(/outside a Jazz session/);
    await expect(result.current.refresh()).rejects.toThrow(/<JazzProvider config>/);
    expect(() => result.current.exportDeviceKey()).toThrow(/outside a Jazz session/);
  });

  it("is stable across renders while the client's auth doesn't change", () => {
    const { result, rerender } = renderHook(() => useJazzSession(), {
      wrapper: wrapperFor(clientWithSession("external")),
    });
    const first = result.current;
    rerender();
    expect(result.current).toBe(first);
  });

  it("reads its own client, not an enclosing <JazzProvider>'s session", async () => {
    const outerSignOut = vi.fn(async () => {});
    const outer = { status: "ready", user: { accountId: "outer" }, signOut: outerSignOut };
    const inner = clientWithSession("external");
    const { result } = renderHook(() => useJazzSession(), {
      wrapper: ({ children }) => (
        <SessionContext.Provider value={outer}>
          <JazzClientProvider client={inner as any}>{children}</JazzClientProvider>
        </SessionContext.Provider>
      ),
    });
    expect(result.current.client).toBe(inner);
    expect(result.current.user?.accountId).toBe("00000000-0000-4000-8000-000000000002");
    await expect(result.current.signOut()).rejects.toThrow(/outside a Jazz session/);
    expect(outerSignOut).not.toHaveBeenCalled();
  });

  it("still throws outside any provider", () => {
    expect(() => renderHook(() => useJazzSession())).toThrow(
      /within <JazzProvider> or <JazzClientProvider>/,
    );
  });
});

describe("useSession (deprecated)", () => {
  it("still returns the permission session, and warns once naming useJazzSession().user", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const client = clientWithSession("external");
    const { result, rerender } = renderHook(() => useSession(), { wrapper: wrapperFor(client) });
    rerender();
    expect(result.current?.user).toBe("00000000-0000-4000-8000-000000000002");
    const calls = warn.mock.calls.filter((call) => String(call[0]).includes("useSession()"));
    expect(calls).toHaveLength(1);
    expect(String(calls[0]![0])).toContain("useJazzSession().user");
    warn.mockRestore();
  });
});
