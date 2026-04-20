import { describe, it, expect } from "vitest";
import { appScopedUrl, httpUrlToWs } from "./url.js";

describe("appScopedUrl", () => {
  it("trims the server url, preserves any base path, and normalizes the app-scoped path", () => {
    expect(appScopedUrl(" https://api.example.com/base/ ", "my app", "/admin/schemas")).toBe(
      "https://api.example.com/base/apps/my%20app/admin/schemas",
    );
  });

  it("throws when the server url includes query params or a hash fragment", () => {
    expect(() => appScopedUrl("http://localhost:4000?debug=1", "xyz", "schemas")).toThrow(
      /must not include query parameters or a hash fragment/i,
    );
  });
});

describe("httpUrlToWs", () => {
  it("http → ws and appends app-scoped /ws", () => {
    expect(httpUrlToWs("http://localhost:4000", "xyz")).toBe("ws://localhost:4000/apps/xyz/ws");
  });
  it("https → wss and trims trailing slash", () => {
    expect(httpUrlToWs("https://api.example.com/", "xyz")).toBe(
      "wss://api.example.com/apps/xyz/ws",
    );
  });
  it("ws/wss passthrough and replaces a bare or trailing /ws with the app-scoped path", () => {
    expect(httpUrlToWs("ws://host", "xyz")).toBe("ws://host/apps/xyz/ws");
    expect(httpUrlToWs("wss://host/ws", "xyz")).toBe("wss://host/apps/xyz/ws");
  });
  it("trims surrounding whitespace before normalizing the app-scoped ws endpoint", () => {
    expect(httpUrlToWs(" https://api.example.com/ ", "xyz")).toBe(
      "wss://api.example.com/apps/xyz/ws",
    );
  });
  it("accepts UUID app ids verbatim", () => {
    expect(httpUrlToWs("http://localhost:4000", "00000000-0000-0000-0000-000000000001")).toBe(
      "ws://localhost:4000/apps/00000000-0000-0000-0000-000000000001/ws",
    );
  });
  it("throws on invalid scheme", () => {
    expect(() => httpUrlToWs("ftp://host", "xyz")).toThrow(/Invalid server URL/);
  });
});
