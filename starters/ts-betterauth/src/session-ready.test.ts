import { describe, expect, it } from "vitest";

import { waitForInitialSession } from "./session-ready.js";

describe("waitForInitialSession", () => {
  it("handles a synchronous ready notification without reading an uninitialized unsubscribe", async () => {
    let unsubscribed = false;
    await expect(
      waitForInitialSession<{ isPending: boolean }>({
        get: () => ({ isPending: true }),
        subscribe(listener) {
          listener({ isPending: false });
          return () => {
            unsubscribed = true;
          };
        },
      }),
    ).resolves.toBeUndefined();
    expect(unsubscribed).toBe(true);
  });
});
