import { describe, expect, it, vi } from "vitest";
import {
  keepMountedSession,
  refreshAuthentication,
  type JazzCredentials,
} from "../../../src/model/auth-state.js";

const cached: JazzCredentials = { did: "did:plc:alice", token: "cached-token" };

describe("authentication refresh", () => {
  it("keeps the mounted Jazz session stable when only its JWT is refreshed", () => {
    const refreshed = { did: cached.did, token: "fresh-token" };

    expect(keepMountedSession(cached, refreshed)).toBe(cached);
  });

  it("mounts new Jazz credentials when the authenticated DID changes", () => {
    const refreshed = { did: "did:plc:bob", token: "bob-token" };

    expect(keepMountedSession(cached, refreshed)).toBe(refreshed);
  });

  it("accepts fresh Jazz credentials minted by the BFF", async () => {
    const state = await refreshAuthentication(
      cached,
      async () =>
        Response.json({
          did: cached.did,
          token: "fresh-token",
        }),
      vi.fn(),
    );

    expect(state).toEqual({
      kind: "signed-in",
      session: { did: cached.did, token: "fresh-token" },
    });
  });

  it("clears a cached Jazz JWT after an authoritative rejection", async () => {
    const clear = vi.fn();
    const state = await refreshAuthentication(
      cached,
      async () => new Response(null, { status: 401 }),
      clear,
    );
    expect(state).toEqual({ kind: "signed-out" });
    expect(clear).toHaveBeenCalledOnce();
  });

  it("retains a cached Jazz JWT when the BFF cannot be reached", async () => {
    const clear = vi.fn();
    const state = await refreshAuthentication(
      cached,
      async () => {
        throw new TypeError("offline");
      },
      clear,
    );
    expect(state).toEqual({ kind: "signed-in", session: cached });
    expect(clear).not.toHaveBeenCalled();
  });

  it("reports an unavailable BFF when there are no cached Jazz credentials", async () => {
    const state = await refreshAuthentication(
      undefined,
      async () => {
        throw new TypeError("offline");
      },
      vi.fn(),
    );

    expect(state).toEqual({
      kind: "unavailable",
      message: "Could not reach the BFF to check your session.",
    });
  });

  it("reports an invalid successful response instead of presenting it as signed out", async () => {
    const state = await refreshAuthentication(
      undefined,
      async () =>
        Response.json({
          did: cached.did,
        }),
      vi.fn(),
    );

    expect(state).toEqual({
      kind: "unavailable",
      message: "The BFF returned invalid Jazz credentials.",
    });
  });
});
