import { afterEach, expect, it, vi } from "vitest";
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

it("matches standard server authority canonicalization and rejection", () => {
  const valid = [
    "https://CORE.EXAMPLE/base",
    "https://%41.example/base",
    "https://café.example/base",
    "https://日本語.example",
    "https://xn--caf-dma.example",
    "https://CORE.EXAMPLE:443/base/",
    "http://127.1:80",
    "http://0x7f.0.0.1",
    "https://０１２.０.０.１",
    "https://[2001:0db8::1]:443/base",
    "https://name:secret@CORE.EXAMPLE/base?query=value#fragment",
    "https://core.example/a/../b",
    "https://example.0xg",
    "https://core.example./",
    "https://faß.example",
  ];
  for (const input of valid) expect(new PlatformURL(input).href).toBe(new URL(input).href);
  for (const input of [
    "https://%FF",
    "https://%C0%AF.example",
    "https://%00.example",
    "https://%23.example",
    "https://xn--",
    "https://a.1",
    "https://example.123",
    "https://example.09",
    "https://example.0x",
    "https://[not-ipv6]",
    "https://core.example:99999",
    "https://\u200d.example",
  ]) {
    expect(() => new URL(input)).toThrow();
    expect(() => new PlatformURL(input)).toThrow();
  }
});
