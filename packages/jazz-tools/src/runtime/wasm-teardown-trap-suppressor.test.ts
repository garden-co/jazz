// @vitest-environment happy-dom
import { afterEach, describe, expect, it } from "vitest";
import {
  installWasmTeardownTrapSuppressor,
  isWasmTeardownTrap,
  markWasmTeardownInProgress,
  resetWasmTeardownTrapSuppressorForTest,
} from "./wasm-teardown-trap-suppressor.js";

function dispatchError(message: string): ErrorEvent {
  const event = new window.ErrorEvent("error", { message, cancelable: true });
  window.dispatchEvent(event);
  return event;
}

describe("isWasmTeardownTrap", () => {
  it("matches the wasm trap signatures a corrupted-on-teardown heap produces", () => {
    expect(isWasmTeardownTrap("RuntimeError: memory access out of bounds")).toBe(true);
    expect(isWasmTeardownTrap("RuntimeError: unreachable")).toBe(true);
    expect(isWasmTeardownTrap("table index is out of bounds")).toBe(true);
    expect(isWasmTeardownTrap("null function or function signature mismatch")).toBe(true);
  });

  it("ignores unrelated errors and empty messages", () => {
    expect(isWasmTeardownTrap("TypeError: foo is not a function")).toBe(false);
    expect(isWasmTeardownTrap("")).toBe(false);
    expect(isWasmTeardownTrap(undefined)).toBe(false);
    expect(isWasmTeardownTrap(null)).toBe(false);
  });
});

describe("WASM teardown trap suppressor", () => {
  afterEach(() => {
    resetWasmTeardownTrapSuppressorForTest();
  });

  it("does not suppress the trap before the teardown window opens", () => {
    installWasmTeardownTrapSuppressor();
    const event = dispatchError("RuntimeError: memory access out of bounds");
    expect(event.defaultPrevented).toBe(false);
  });

  it("suppresses the teardown trap once the window is open", () => {
    markWasmTeardownInProgress();
    const event = dispatchError("RuntimeError: memory access out of bounds");
    expect(event.defaultPrevented).toBe(true);
  });

  it("leaves genuine errors untouched even during teardown", () => {
    markWasmTeardownInProgress();
    const event = dispatchError("TypeError: cannot read properties of undefined");
    expect(event.defaultPrevented).toBe(false);
  });

  it("stops the trap reaching app-level error handlers registered after it", () => {
    markWasmTeardownInProgress();
    let appHandlerCalls = 0;
    const appHandler = () => {
      appHandlerCalls++;
    };
    window.addEventListener("error", appHandler, true);
    try {
      dispatchError("RuntimeError: memory access out of bounds");
      expect(appHandlerCalls).toBe(0);
      dispatchError("TypeError: real app error");
      expect(appHandlerCalls).toBe(1);
    } finally {
      window.removeEventListener("error", appHandler, true);
    }
  });
});
