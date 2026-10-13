import { afterEach, expect, it, vi } from "vitest";
import { isBrowserHostRuntime } from "./browser-host.js";

afterEach(() => vi.unstubAllGlobals());

it("is false in Node even with window, document, localStorage and Web Locks", () => {
  vi.stubGlobal("window", {});
  vi.stubGlobal("document", {});
  vi.stubGlobal("localStorage", {});
  vi.stubGlobal("navigator", { locks: {} });
  expect(isBrowserHostRuntime()).toBe(false);
});

it("is true for a browser page without Node's process", () => {
  vi.stubGlobal("process", undefined);
  vi.stubGlobal("window", {});
  vi.stubGlobal("document", {});
  expect(isBrowserHostRuntime()).toBe(true);
});

it("is false for React Native", () => {
  vi.stubGlobal("process", undefined);
  vi.stubGlobal("window", {});
  vi.stubGlobal("document", {});
  vi.stubGlobal("navigator", { product: "ReactNative" });
  expect(isBrowserHostRuntime()).toBe(false);
});
