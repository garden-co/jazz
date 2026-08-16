import { afterEach, describe, expect, it, vi } from "vitest";
import {
  resetClientRegistryForTest,
  acquireClient,
  releaseClient,
  waitForClientRegistryIdleForTest,
} from "./client-registry.js";

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

  it("lets tests wait for deferred async teardown to finish", async () => {
    let finishShutdown!: () => void;
    const client = {
      shutdown: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            finishShutdown = resolve;
          }),
      ),
    };
    const holder = {};
    await acquireClient("k", async () => client, holder);

    void releaseClient("k", holder);
    const idle = waitForClientRegistryIdleForTest();
    await vi.waitFor(() => expect(client.shutdown).toHaveBeenCalledOnce());

    let idleResolved = false;
    void idle.then(() => {
      idleResolved = true;
    });
    await Promise.resolve();
    expect(idleResolved).toBe(false);

    finishShutdown();
    await idle;
    expect(idleResolved).toBe(true);
  });

  it("waits for a started teardown before creating a replacement", async () => {
    let finishShutdown!: () => void;
    const shutdownFinished = new Promise<void>((resolve) => {
      finishShutdown = resolve;
    });
    const first = { shutdown: vi.fn(() => shutdownFinished) };
    const second = fakeClient();
    const create = vi.fn(async () => (create.mock.calls.length === 1 ? first : second));
    const holder = {};
    await acquireClient("k", create, holder);

    const releasing = releaseClient("k", holder);
    await vi.waitFor(() => expect(first.shutdown).toHaveBeenCalledOnce());

    const replacement = acquireClient("k", create, {});
    expect(create).toHaveBeenCalledTimes(1);

    finishShutdown();
    await expect(replacement).resolves.toBe(second);
    await releasing;
    expect(create).toHaveBeenCalledTimes(2);
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
