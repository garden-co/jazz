/**
 * Unit tests for jazz-worker URL normalization and auth-merge helpers.
 *
 * These tests target the exported pure helpers `composeConnectUrl` and
 * `mergeAuth`, which encapsulate the two behaviours that T22 validates:
 *
 *   1. serverUrl + serverPathPrefix → correct WebSocket URL via httpUrlToWs
 *   2. update-auth merges / clears jwt_token while preserving other fields
 *
 * The helpers are pure functions — no WASM, no worker globals needed.
 * We must still stub `self` and `jazz-wasm` because jazz-worker.ts installs
 * `self.onmessage` and calls `startup()` at module top level.
 */

import { describe, it, expect, vi } from "vitest";

// ── Stub the worker global `self` before the module is loaded ────────────────
// jazz-worker.ts does `self.onmessage = ...` at module scope.
// Node doesn't have a worker `self`; provide a minimal stand-in.
vi.hoisted(() => {
  const fakeSelf = {
    onmessage: null as null | ((e: MessageEvent) => void),
    postMessage: vi.fn(),
    close: vi.fn(),
    location: { origin: "http://localhost", href: "http://localhost/worker.js" },
  };
  (globalThis as Record<string, unknown>).self = fakeSelf;
});

// ── Stub jazz-wasm so startup() doesn't reject ───────────────────────────────
vi.mock("jazz-wasm", () => ({
  default: vi.fn().mockResolvedValue(undefined),
  initSync: vi.fn(),
}));

import {
  composeConnectUrl,
  mergeAuth,
  performUpstreamConnect,
  handleUpdateAuth,
} from "./jazz-worker.js";
import type { WorkerToMainMessage } from "./worker-protocol.js";

describe("worker URL + auth wiring", () => {
  it("normalizes serverUrl with serverPathPrefix via httpUrlToWs", () => {
    const wsUrl = composeConnectUrl("http://localhost:4000", "/apps/xyz");
    expect(wsUrl).toBe("ws://localhost:4000/apps/xyz/ws");
  });

  it("merges new jwtToken into cached auth on update-auth", () => {
    // Simulate state after init: admin_secret cached, initial jwt_token set.
    const afterInit = mergeAuth({ admin_secret: "s" }, "initial");
    expect(afterInit).toEqual({ admin_secret: "s", jwt_token: "initial" });

    // Simulate update-auth arriving with a refreshed token.
    const afterUpdate = mergeAuth(afterInit, "refreshed");
    expect(afterUpdate.jwt_token).toBe("refreshed");
    expect(afterUpdate.admin_secret).toBe("s");
  });

  it("clears jwt_token when update-auth arrives without one", () => {
    // State after init with a token.
    const afterInit = mergeAuth({ admin_secret: "s" }, "initial");

    // update-auth with no jwtToken → token must be removed.
    const afterUpdate = mergeAuth(afterInit, undefined);
    expect(afterUpdate.jwt_token).toBeUndefined();
    expect(afterUpdate.admin_secret).toBe("s");
  });
});

describe("worker update-auth error propagation", () => {
  it("posts auth-failed with reason=invalid when runtime.updateAuth throws", () => {
    const posted: any[] = [];
    const runtime = {
      updateAuth: vi.fn(() => {
        throw new Error("boom");
      }),
    };
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    handleUpdateAuth(runtime, '{"jwt_token":"new.jwt"}', (msg) => posted.push(msg));

    const authFailed = posted.find((m) => m.type === "auth-failed");
    expect(authFailed).toBeDefined();
    expect(authFailed.reason).toBe("invalid");
    errorSpy.mockRestore();
  });
});

describe("performUpstreamConnect", () => {
  it("posts upstream-connected after runtime.connect succeeds", () => {
    const connect = vi.fn();
    const posted: WorkerToMainMessage[] = [];

    performUpstreamConnect(
      { connect },
      (msg) => posted.push(msg),
      "ws://example/ws",
      '{"jwt_token":"x"}',
    );

    expect(connect).toHaveBeenCalledWith("ws://example/ws", '{"jwt_token":"x"}');
    expect(posted).toEqual([{ type: "upstream-connected" }]);
  });

  it("posts upstream-disconnected when runtime.connect throws", () => {
    const connect = vi.fn(() => {
      throw new Error("ws handshake failed");
    });
    const posted: WorkerToMainMessage[] = [];
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    performUpstreamConnect({ connect }, (msg) => posted.push(msg), "ws://example/ws", "{}");

    expect(posted).toEqual([{ type: "upstream-disconnected" }]);
    errorSpy.mockRestore();
  });
});
