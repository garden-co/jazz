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

  it("keeps the original failed shutdown barrier through chained abandoned handoffs", async () => {
    vi.useFakeTimers();
    let failShutdown!: (error: Error) => void;
    const shutdownFinished = new Promise<void>((_, reject) => {
      failShutdown = reject;
    });
    const shutdownError = new Error("shutdown failed");
    const first = { shutdown: vi.fn(() => shutdownFinished) };
    const recovered = fakeClient();
    const create = vi
      .fn<() => Promise<{ shutdown(): Promise<void> }>>()
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce(recovered);
    const firstHolder = {};
    const handoffHolder = {};
    await acquireClient("persistent:chain", create, firstHolder);

    const firstRelease = releaseClient("persistent:chain", firstHolder);
    await vi.runOnlyPendingTimersAsync();
    expect(first.shutdown).toHaveBeenCalledOnce();

    const abandonedHandoff = acquireClient("persistent:chain", create, handoffHolder);
    const handoffRelease = releaseClient("persistent:chain", handoffHolder);
    await vi.runOnlyPendingTimersAsync();
    const laterHandoff = acquireClient("persistent:chain", create, {});

    failShutdown(shutdownError);
    await expect(firstRelease).resolves.toBeUndefined();
    await expect(handoffRelease).resolves.toBeUndefined();
    await expect(abandonedHandoff).rejects.toBe(shutdownError);
    await expect(laterHandoff).rejects.toBe(shutdownError);

    await expect(acquireClient("persistent:chain", create, {})).rejects.toBe(shutdownError);
    expect(create).toHaveBeenCalledTimes(1);
  });

  it("keeps a failed replacement shutdown as the barrier after a successful handoff", async () => {
    vi.useFakeTimers();
    let finishFirstShutdown!: () => void;
    const firstShutdown = new Promise<void>((resolve) => {
      finishFirstShutdown = resolve;
    });
    let failSecondShutdown!: (error: Error) => void;
    const secondShutdown = new Promise<void>((_, reject) => {
      failSecondShutdown = reject;
    });
    const shutdownError = new Error("replacement shutdown failed");
    const first = { shutdown: vi.fn(() => firstShutdown) };
    const second = { shutdown: vi.fn(() => secondShutdown) };
    const create = vi
      .fn<() => Promise<{ shutdown(): Promise<void> }>>()
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce(second);
    const firstHolder = {};
    const secondHolder = {};
    await acquireClient("persistent:replacement-chain", create, firstHolder);

    const firstRelease = releaseClient("persistent:replacement-chain", firstHolder);
    await vi.runOnlyPendingTimersAsync();
    expect(first.shutdown).toHaveBeenCalledOnce();

    const handoff = acquireClient("persistent:replacement-chain", create, secondHolder);
    finishFirstShutdown();
    await expect(handoff).resolves.toBe(second);
    await expect(firstRelease).resolves.toBeUndefined();

    const secondRelease = releaseClient("persistent:replacement-chain", secondHolder);
    await vi.runOnlyPendingTimersAsync();
    expect(second.shutdown).toHaveBeenCalledOnce();

    const waitingAcquire = acquireClient("persistent:replacement-chain", create, {});
    failSecondShutdown(shutdownError);

    await expect(secondRelease).resolves.toBeUndefined();
    await expect(waitingAcquire).rejects.toBe(shutdownError);
    await expect(acquireClient("persistent:replacement-chain", create, {})).rejects.toBe(
      shutdownError,
    );
    expect(create).toHaveBeenCalledTimes(2);
  });

  it("retains the failed shutdown barrier across the release timer's first microtask", async () => {
    vi.useFakeTimers();
    let failShutdown!: (error: Error) => void;
    const shutdownFinished = new Promise<void>((_, reject) => {
      failShutdown = reject;
    });
    const shutdownError = new Error("shutdown failed");
    const first = { shutdown: vi.fn(() => shutdownFinished) };
    const recovered = fakeClient();
    const create = vi
      .fn<() => Promise<{ shutdown(): Promise<void> }>>()
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce(recovered);
    const holder = {};
    await acquireClient("persistent:first-microtask", create, holder);

    const releasing = releaseClient("persistent:first-microtask", holder);
    vi.advanceTimersByTime(0);
    const handoff = acquireClient("persistent:first-microtask", create, {});
    await Promise.resolve();
    expect(first.shutdown).toHaveBeenCalledOnce();

    failShutdown(shutdownError);
    await expect(releasing).resolves.toBeUndefined();
    await expect(handoff).rejects.toBe(shutdownError);
    await expect(acquireClient("persistent:first-microtask", create, {})).rejects.toBe(
      shutdownError,
    );
    expect(create).toHaveBeenCalledTimes(1);
  });

  it("cleans up an abandoned handoff after its predecessor shuts down", async () => {
    vi.useFakeTimers();
    let finishShutdown!: () => void;
    const shutdownFinished = new Promise<void>((resolve) => {
      finishShutdown = resolve;
    });
    const first = { shutdown: vi.fn(() => shutdownFinished) };
    const replacement = fakeClient();
    const create = vi
      .fn<() => Promise<{ shutdown(): Promise<void> }>>()
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce(replacement)
      .mockResolvedValueOnce(fakeClient());
    const firstHolder = {};
    const handoffHolder = {};
    await acquireClient("persistent:successful-chain", create, firstHolder);

    const firstRelease = releaseClient("persistent:successful-chain", firstHolder);
    await vi.runOnlyPendingTimersAsync();
    expect(first.shutdown).toHaveBeenCalledOnce();

    const abandonedHandoff = acquireClient("persistent:successful-chain", create, handoffHolder);
    const handoffRelease = releaseClient("persistent:successful-chain", handoffHolder);
    await vi.runOnlyPendingTimersAsync();

    finishShutdown();
    await expect(abandonedHandoff).resolves.toBe(replacement);
    await expect(firstRelease).resolves.toBeUndefined();
    await expect(handoffRelease).resolves.toBeUndefined();
    expect(replacement.shutdown).toHaveBeenCalledOnce();

    await expect(acquireClient("persistent:successful-chain", create, {})).resolves.toBeTruthy();
    expect(create).toHaveBeenCalledTimes(3);
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
