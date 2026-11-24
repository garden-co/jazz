import { describe, expect, it } from "vitest";
import { durableObjects } from "./durable-objects-provider.js";

const secret = "s".repeat(32);

describe("durableObjects()", () => {
  it("keeps the Worker's origin and the core secret", () => {
    expect(durableObjects({ url: "https://my-app-jazz.example.workers.dev/", secret })).toEqual({
      kind: "durableObjects",
      url: "https://my-app-jazz.example.workers.dev",
      secret,
    });
  });

  it.each([
    ["not a URL", "my-app-jazz.example.workers.dev"],
    ["a path", "https://my-app-jazz.example.workers.dev/jazz"],
    ["a query", "https://my-app-jazz.example.workers.dev/?x=1"],
    ["another scheme", "wss://my-app-jazz.example.workers.dev"],
  ])("refuses a url that is %s", (_, url) => {
    expect(() => durableObjects({ url, secret })).toThrow(/url must be the Worker's origin/);
  });

  it("refuses a url with a user name or password", () => {
    expect(() =>
      durableObjects({ url: "https://user:pass@my-app-jazz.example.workers.dev", secret }),
    ).toThrow(/must not carry a user name or password/);
  });

  it("takes http:// only for this machine", () => {
    expect(() => durableObjects({ url: "http://my-app-jazz.example.workers.dev", secret })).toThrow(
      /url must be https:\/\//,
    );
    for (const url of ["http://localhost:8787", "http://127.0.0.1:8787", "http://[::1]:8787"]) {
      expect(durableObjects({ url, secret }).url).toBe(url);
    }
  });

  it("refuses an appId: the core keeps its own", () => {
    expect(() =>
      durableObjects({ url: "https://w.example.workers.dev", secret, appId: "x" } as never),
    ).toThrow(/appId is not an option: the store keeps its own app id/);
  });

  it("refuses a secret shorter than 32 characters", () => {
    expect(() => durableObjects({ url: "https://w.example.workers.dev", secret: "short" })).toThrow(
      /JAZZ_CORE_SECRET, a random string of at least 32 characters/,
    );
  });
});
