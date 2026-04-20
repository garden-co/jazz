import { describe, it, expect } from "vitest";
import { httpUrlToWs } from "./url.js";

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
  it("accepts UUID app ids verbatim", () => {
    expect(httpUrlToWs("http://localhost:4000", "00000000-0000-0000-0000-000000000001")).toBe(
      "ws://localhost:4000/apps/00000000-0000-0000-0000-000000000001/ws",
    );
  });
  it("throws on invalid scheme", () => {
    expect(() => httpUrlToWs("ftp://host", "xyz")).toThrow(/Invalid server URL/);
  });
});
