import { describe, expect, it } from "vitest";
import { durableObjects } from "./create-jazz.js";

const secret = "s".repeat(32);

describe("durableObjects()", () => {
  it("reaches the Worker as a remote core holding the core secret", async () => {
    const store = durableObjects({
      url: "https://my-app-jazz.example.workers.dev/",
      appId: "my-app",
      secret,
    });
    expect(store).toMatchObject({
      kind: "remote",
      host: "durable-objects",
      url: "https://my-app-jazz.example.workers.dev",
      appId: "my-app",
      secret,
    });
    expect(await store.backendCredential()).toBe(secret);
  });

  it.each([
    ["not a URL", "my-app-jazz.example.workers.dev"],
    ["a path", "https://my-app-jazz.example.workers.dev/jazz"],
    ["a query", "https://my-app-jazz.example.workers.dev/?x=1"],
    ["another scheme", "wss://my-app-jazz.example.workers.dev"],
  ])("refuses a url that is %s", (_, url) => {
    expect(() => durableObjects({ url, appId: "my-app", secret })).toThrow(
      /url must be the Worker's origin/,
    );
  });

  it("refuses an empty appId", () => {
    expect(() =>
      durableObjects({ url: "https://w.example.workers.dev", appId: " ", secret }),
    ).toThrow(/appId must be the Worker's JAZZ_APP_ID/);
  });

  it("refuses a secret shorter than 32 characters", () => {
    expect(() =>
      durableObjects({ url: "https://w.example.workers.dev", appId: "a", secret: "short" }),
    ).toThrow(/JAZZ_CORE_SECRET, a random string of at least 32 characters/);
  });
});
