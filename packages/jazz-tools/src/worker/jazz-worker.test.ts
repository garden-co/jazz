/**
 * Unit tests for jazz-worker URL normalization, auth-merge helpers, and
 * handleInit fallback behaviour.
 *
 * Pure helper tests (composeConnectUrl, mergeAuth, etc.) need no WASM.
 * The handleInit test drives the full init flow via self.onmessage and a
 * mocked WasmRuntime so we can exercise the SecurityError fallback path
 * without a real browser or OPFS.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

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

// ── WasmRuntime mocks (hoisted so vi.mock factory can close over them) ────────
const { openPersistentMock, openEphemeralMock } = vi.hoisted(() => ({
  openPersistentMock: vi.fn(),
  openEphemeralMock: vi.fn(),
}));

// ── Stub jazz-wasm so startup() doesn't reject ───────────────────────────────
vi.mock("jazz-wasm", () => ({
  default: vi.fn().mockResolvedValue(undefined),
  initSync: vi.fn(),
  WasmRuntime: {
    openPersistent: openPersistentMock,
    openEphemeral: openEphemeralMock,
  },
}));

// ── Stub schema-wire so handleInit doesn't fail on schema validation ──────────
vi.mock("../drivers/schema-wire.js", () => ({
  normalizeRuntimeSchemaJson: vi.fn((s: string) => s),
}));

import {
  composeConnectUrl,
  mergeAuth,
  performUpstreamConnect,
  handleUpdateAuth,
} from "./jazz-worker.js";
import type { WorkerToMainMessage } from "./worker-protocol.js";

describe("worker URL + auth wiring", () => {
  it("normalizes serverUrl with appId via httpUrlToWs", () => {
    const wsUrl = composeConnectUrl("http://localhost:4000", "xyz");
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

// ── Firefox private browsing: OPFS unavailable ────────────────────────────────
//
// navigator.storage.getDirectory() is blocked in Firefox private browsing,
// causing WasmRuntime.openPersistent to throw a SecurityError. The worker
// should detect this and fall back to WasmRuntime.openEphemeral so that Jazz
// still initialises (with ephemeral, non-persisted storage) instead of failing.
//
// This test will fail until the fallback is implemented in handleInit.

describe("handleInit — OPFS unavailable (Firefox private browsing)", () => {
  const fakeSelf = () => (globalThis as any).self;

  const fakeRuntime = () => ({
    addClient: vi.fn().mockReturnValue("client-ephemeral"),
    setClientRole: vi.fn(),
    onAuthFailure: null,
    onSyncMessageToSend: vi.fn(),
    addServer: vi.fn(),
    removeServer: vi.fn(),
  });

  beforeEach(() => {
    openPersistentMock.mockReset();
    openEphemeralMock.mockReset();
    fakeSelf().postMessage.mockClear();
  });

  it("falls back to openEphemeral and posts init-ok when openPersistent throws SecurityError", async () => {
    openPersistentMock.mockRejectedValue(
      new DOMException("The operation is insecure.", "SecurityError"),
    );
    openEphemeralMock.mockResolvedValue(fakeRuntime());

    fakeSelf().onmessage(
      new MessageEvent("message", {
        data: {
          type: "init",
          schemaJson: "{}",
          appId: "opfs-blocked-test",
          env: "development",
          userBranch: "main",
          dbName: "opfs-blocked-db",
          clientId: "",
        },
      }),
    );

    await vi.waitUntil(
      () =>
        (fakeSelf().postMessage.mock.calls as [any][]).some(
          ([msg]) => msg.type === "init-ok" || msg.type === "error",
        ),
      { timeout: 2000 },
    );

    const result: any = (fakeSelf().postMessage.mock.calls as [any][])
      .map(([msg]) => msg)
      .find((msg: any) => msg.type === "init-ok" || msg.type === "error");

    expect(result.type).toBe("init-ok");
  });
});
