import { afterEach, describe, expect, it, vi } from "vitest";
import { resetClientRegistryForTest, acquireClient, releaseClient } from "./client-registry.js";

function fakeClient() {
  return { shutdown: vi.fn(async () => undefined) };
}

afterEach(() => {
  resetClientRegistryForTest();
  vi.useRealTimers();
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

  it("shares the pending release promise across repeated releases", async () => {
    const client = fakeClient();
    const holder = {};
    await acquireClient("k", async () => client, holder);

    const firstRelease = releaseClient("k", holder);
    const repeatedRelease = releaseClient("k", holder);
    let repeatedSettled = false;
    void repeatedRelease.then(() => {
      repeatedSettled = true;
    });

    await Promise.resolve();
    expect(repeatedSettled).toBe(false);

    await firstRelease;
    await repeatedRelease;
    expect(client.shutdown).toHaveBeenCalledOnce();
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

  it("keeps a failed shutdown as the closing barrier for later acquires", async () => {
    let failShutdown!: (error: Error) => void;
    const shutdownFinished = new Promise<void>((_, reject) => {
      failShutdown = reject;
    });
    const shutdownError = new Error("shutdown failed");
    const first = { shutdown: vi.fn(() => shutdownFinished) };
    const second = fakeClient();
    const create = vi.fn(async () => (create.mock.calls.length === 1 ? first : second));
    const holder = {};
    await acquireClient("persistent:k", create, holder);

    const releasing = releaseClient("persistent:k", holder);
    await vi.waitFor(() => expect(first.shutdown).toHaveBeenCalledOnce());

    const waitingAcquire = acquireClient("persistent:k", create, {});
    expect(create).toHaveBeenCalledTimes(1);

    failShutdown(shutdownError);
    await expect(releasing).resolves.toBeUndefined();
    await expect(waitingAcquire).rejects.toBe(shutdownError);
    await expect(acquireClient("persistent:k", create, {})).rejects.toBe(shutdownError);
    expect(create).toHaveBeenCalledTimes(1);
  });

  it("retries after a pending creation fails during release and re-acquire", async () => {
    vi.useFakeTimers();
    let failCreate!: (error: Error) => void;
    const pendingCreate = new Promise<{ shutdown(): Promise<void> }>((_, reject) => {
      failCreate = reject;
    });
    const recovered = fakeClient();
    const create = vi
      .fn<() => Promise<{ shutdown(): Promise<void> }>>()
      .mockReturnValueOnce(pendingCreate)
      .mockResolvedValueOnce(recovered);
    const holder = {};

    const initial = acquireClient("pending:k", create, holder);
    const releasing = releaseClient("pending:k", holder);
    await vi.runOnlyPendingTimersAsync();

    const overlappingAcquire = acquireClient("pending:k", create, {});
    expect(create).toHaveBeenCalledTimes(1);

    const createError = new Error("create failed");
    failCreate(createError);
    await expect(initial).rejects.toBe(createError);
    await expect(releasing).resolves.toBeUndefined();
    await expect(overlappingAcquire).rejects.toBe(createError);

    await expect(acquireClient("pending:k", create, {})).resolves.toBe(recovered);
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
