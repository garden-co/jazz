import { afterEach, expect, it, vi } from "vitest";

const platform = vi.hoisted(() => ({ OS: "ios" }));
vi.mock("react-native", () => ({ Platform: platform }));
const relay = vi.hoisted(() => ({ loaded: vi.fn() }));
vi.mock("jazz-rn/relay", () => {
  relay.loaded();
  return { installNativeForegroundRuntime: vi.fn() };
});

vi.mock("expo-crypto", () => ({ CryptoDigestAlgorithm: { SHA256: "SHA-256" } }));
vi.mock("expo-secure-store", () => ({}));

afterEach(() => {
  platform.OS = "ios";
  vi.resetModules();
  relay.loaded.mockClear();
});

it("refuses the web target before loading the relay", async () => {
  platform.OS = "web";
  const { importNativeRelay } = await import("./native-relay.js");
  await expect(importNativeRelay()).rejects.toThrow(
    "runs only in iOS and Android builds, not on the web target. For web, use the browser client from jazz-tools/react",
  );
  expect(relay.loaded).not.toHaveBeenCalled();
});

it("loads the relay on a native platform", async () => {
  const { importNativeRelay } = await import("./native-relay.js");
  await expect(importNativeRelay()).resolves.toHaveProperty("installNativeForegroundRuntime");
  expect(relay.loaded).toHaveBeenCalled();
});

it("fails an Expo client on the web target with the same error", async () => {
  platform.OS = "web";
  const { createAccountManager } = await import("../expo/create-account-manager.js");
  await expect(
    createAccountManager({ serverUrl: "https://app.example.test/jazz" }),
  ).rejects.toThrow("not on the web target");
});
