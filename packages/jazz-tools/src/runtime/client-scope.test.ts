import { afterEach, describe, expect, it } from "vitest";
import { clientScope, endpointScope } from "./client-scope.js";

const previous = (globalThis as { location?: unknown }).location;
afterEach(() => {
  (globalThis as { location?: unknown }).location = previous;
});

describe("clientScope", () => {
  it("is the app id where backend or test code names one", () => {
    expect(clientScope({ appId: "my-app", scope: "old", serverUrl: "/jazz" })).toBe("my-app");
  });

  it("is a legacy app's scope, so its users keep their secrets and accounts", () => {
    expect(clientScope({ scope: "old-app-id", serverUrl: "https://app.example.com/jazz" })).toBe(
      "old-app-id",
    );
  });

  it("is otherwise the endpoint the client syncs with", () => {
    (globalThis as { location?: unknown }).location = { href: "https://app.example.com/page" };
    expect(clientScope({ serverUrl: "/jazz" })).toBe("https://app.example.com/jazz");
    expect(clientScope({ serverUrl: "/jazz/" })).toBe("https://app.example.com/jazz");
    expect(clientScope({ serverUrl: "wss://API.example.com:443/jazz" })).toBe(
      "https://api.example.com/jazz",
    );
    expect(clientScope({ serverUrl: "http://localhost:5173/jazz" })).toBe(
      "http://localhost:5173/jazz",
    );
  });

  it("is local for a client with no server", () => {
    expect(clientScope({})).toBe("local");
  });

  it("refuses a server URL that isn't http(s) or ws(s)", () => {
    expect(() => endpointScope("ftp://example.com/jazz")).toThrow(/Invalid server URL/);
  });
});
