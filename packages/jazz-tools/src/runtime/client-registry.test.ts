import { afterEach, describe, expect, it, vi } from "vitest";
import { resetClientRegistryForTest, acquireClient, releaseClient } from "./client-registry.js";

function fakeClient() {
  return { shutdown: vi.fn(async () => undefined) };
}

afterEach(() => {
  resetClientRegistryForTest();
});

describe("client-registry", () => {
  it("shares one instance for the same key (create runs once)", async () => {
    const create = vi.fn(async () => fakeClient());
    const a = await acquireClient("k", create, {});
    const b = await acquireClient("k", create, {});
    expect(create).toHaveBeenCalledTimes(1);
    expect(a).toBe(b);
  });

  it("keeps distinct keys on separate instances", async () => {
    const create = vi.fn(async () => fakeClient());
    await acquireClient("a", create, {});
    await acquireClient("b", create, {});
    expect(create).toHaveBeenCalledTimes(2);
  });

  it("shuts down only once the last holder releases", async () => {
    const client = fakeClient();
    const create = vi.fn(async () => client);
    const h1 = {};
    const h2 = {};
    await acquireClient("k", create, h1);
    await acquireClient("k", create, h2);

    await releaseClient("k", h1);
    expect(client.shutdown).not.toHaveBeenCalled();

    await releaseClient("k", h2);
    expect(client.shutdown).toHaveBeenCalledTimes(1);
  });

  it("cancels teardown when a holder re-acquires within the release window", async () => {
    const client = fakeClient();
    const create = vi.fn(async () => client);
    const h1 = {};
    await acquireClient("k", create, h1);

    const releasing = releaseClient("k", h1); // schedules deferred teardown
    const reacquired = await acquireClient("k", create, {}); // cancels it
    await releasing;

    expect(client.shutdown).not.toHaveBeenCalled();
    expect(create).toHaveBeenCalledTimes(1);
    expect(reacquired).toBe(client);
  });

  it("evicts a failed creation so the next acquire retries", async () => {
    const err = new Error("create failed");
    const create = vi
      .fn<() => Promise<{ shutdown: () => Promise<void> }>>()
      .mockRejectedValueOnce(err)
      .mockResolvedValueOnce(fakeClient());

    await expect(acquireClient("k", create, {})).rejects.toBe(err);
    const ok = await acquireClient("k", create, {});

    expect(create).toHaveBeenCalledTimes(2);
    expect(ok).toBeTruthy();
  });
});
