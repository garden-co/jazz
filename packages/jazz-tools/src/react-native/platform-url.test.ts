import { afterEach, expect, it, vi } from "vitest";
vi.mock("react-native", () => ({ NativeModules: {}, Platform: { OS: "android", Version: 35 } }));
vi.mock("../runtime/platform-url.js", () => import("../runtime/platform-url.native.js"));
import { accountRegistryUrl } from "../accounts/context.js";
import { PlatformURL } from "../runtime/platform-url.native.js";
afterEach(() => vi.unstubAllGlobals());

it("uses canonical account authority URLs even when the native global adds trailing slashes", () => {
  const HostURL = globalThis.URL;
  vi.stubGlobal(
    "URL",
    class extends HostURL {
      get pathname() {
        return super.pathname.replace(/\/?$/, "/");
      }
    },
  );
  const appId = "a1510000-0000-4000-8000-000000000001";
  const expected = `https://core.example/base/apps/${appId}/accounts`;
  expect(accountRegistryUrl("wss://core.example/base/", appId)).toBe(expected);
  expect(new PlatformURL(expected).pathname).toBe(`/base/apps/${appId}/accounts`);
  expect(accountRegistryUrl("https://other.example/base", appId)).not.toBe(expected);
  for (const base of [
    "https://name:secret@core.example",
    "https://core.example?tenant=other",
    "https://core.example#other",
    "file:///core",
  ]) {
    expect(() => accountRegistryUrl(base, appId)).toThrow("invalid_registry_url");
  }
});
