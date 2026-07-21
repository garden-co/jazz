import { describe, expect, it, vi } from "vitest";
import { keepMountedSession, refreshAuthentication, type Session } from "../../src/auth-state.js";

const cached: Session = { did: "did:plc:alice", token: "cached-token" };

describe("authentication refresh", () => {
  it("keeps the mounted Jazz session stable when only its JWT is refreshed", () => {
    const refreshed = { did: cached.did, token: "fresh-token" };

    expect(keepMountedSession(cached, refreshed)).toBe(cached);
  });

  it("clears a cached Jazz JWT after an authoritative rejection", async () => {
    const clear = vi.fn();
    const state = await refreshAuthentication(cached, async () => new Response(null, { status: 401 }), clear);
    expect(state).toEqual({ kind: "signed-out" });
    expect(clear).toHaveBeenCalledOnce();
  });

  it("retains a cached Jazz JWT when the BFF cannot be reached", async () => {
    const clear = vi.fn();
    const state = await refreshAuthentication(cached, async () => {
      throw new TypeError("offline");
    }, clear);
    expect(state).toEqual({ kind: "signed-in", session: cached });
    expect(clear).not.toHaveBeenCalled();
  });
});
