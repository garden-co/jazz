import { describe, it, expect } from "vitest";
import { httpUrlToWs } from "./url.js";

describe("httpUrlToWs", () => {
  it("http → ws and appends /ws", () => {
    expect(httpUrlToWs("http://localhost:4000")).toBe("ws://localhost:4000/ws");
  });
  it("https → wss and trims trailing slash", () => {
    expect(httpUrlToWs("https://api.example.com/")).toBe("wss://api.example.com/ws");
  });
  it("ws/wss passthrough and idempotent /ws suffix", () => {
    expect(httpUrlToWs("ws://host")).toBe("ws://host/ws");
    expect(httpUrlToWs("wss://host/ws")).toBe("wss://host/ws");
  });
  it("applies pathPrefix with leading slash handling", () => {
    expect(httpUrlToWs("http://localhost:4000", "/apps/xyz")).toBe(
      "ws://localhost:4000/apps/xyz/ws",
    );
  });
  it("applies pathPrefix tolerating extra slashes", () => {
    expect(httpUrlToWs("http://localhost:4000", "apps/xyz/")).toBe(
      "ws://localhost:4000/apps/xyz/ws",
    );
  });
  it("empty or undefined pathPrefix behaves like no prefix", () => {
    expect(httpUrlToWs("http://localhost:4000", "")).toBe("ws://localhost:4000/ws");
    expect(httpUrlToWs("http://localhost:4000", undefined)).toBe("ws://localhost:4000/ws");
  });
  it("throws on invalid scheme", () => {
    expect(() => httpUrlToWs("ftp://host")).toThrow(/Invalid server URL/);
  });
});
