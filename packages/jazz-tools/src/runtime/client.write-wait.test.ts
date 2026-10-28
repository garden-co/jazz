import { afterEach, describe, expect, it, vi } from "vitest";
import { setCredentialRenewal, WriteHandle, WriteWaitTimeoutError } from "./client.js";
import type { AuthFailureReason } from "./auth-state.js";
import {
  JazzClient,
  runtimeTransactionRecordStubs,
  testTxId,
  type AppContext,
  type Runtime,
} from "./client-tests/support.js";

// A runtime whose Global waits stay pending until settled or aborted, as a
// wait held through an outage or a credential refusal does (#3923).
function heldWaitClient() {
  let authFailure: ((reason: string) => void) | null = null;
  const signals: AbortSignal[] = [];
  const settle: Array<() => void> = [];
  const endCredentialWait = vi.fn();
  const runtime = {
    ...runtimeTransactionRecordStubs,
    endCredentialWait,
    onAuthFailure: (callback: (reason: string) => void) => {
      authFailure = callback;
    },
    waitForTransaction: (_txId: unknown, _tier: string, options?: { signal?: AbortSignal }) =>
      new Promise<void>((resolve, reject) => {
        const signal = options?.signal;
        if (signal) {
          signals.push(signal);
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        }
        settle.push(resolve);
      }),
  } as unknown as Runtime;
  const context: AppContext = {
    appId: "test-app",
    schema: {},
    serverUrl: "http://localhost:1625",
  };
  const onAuthFailure = vi.fn<(reason: AuthFailureReason) => void>();
  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      options: { onAuthFailure: (reason: AuthFailureReason) => void },
    ): JazzClient;
  };
  const client = new JazzClientCtor(runtime, context, { onAuthFailure });
  const handle = new WriteHandle(testTxId("tx-1"), client);
  return {
    client,
    handle,
    signals,
    onAuthFailure,
    endCredentialWait,
    refuse: (reason: string) => authFailure?.(reason),
    settleAll: () => settle.splice(0).forEach((resolve) => resolve()),
  };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

function track(promise: Promise<unknown>) {
  const state: { value: unknown } = { value: "pending" };
  promise.then(
    () => (state.value = "resolved"),
    (error: unknown) => (state.value = error),
  );
  return state;
}

describe("write waits (#3923)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("rejects with WriteWaitTimeoutError and releases the runtime wait", async () => {
    vi.useFakeTimers();
    const { handle, signals } = heldWaitClient();
    const wait = track(handle.wait({ tier: "global", timeoutMs: 1_000 }));
    await vi.advanceTimersByTimeAsync(999);
    expect(wait.value).toBe("pending");
    await vi.advanceTimersByTimeAsync(1);
    expect(wait.value).toBeInstanceOf(WriteWaitTimeoutError);
    expect(wait.value).toMatchObject({ code: "timeout", tier: "global", timeoutMs: 1_000 });
    expect(signals).toHaveLength(1);
    expect(signals[0]!.aborted).toBe(true);
  });

  it("releases the runtime wait once the write settles", async () => {
    const { handle, signals, settleAll } = heldWaitClient();
    const wait = handle.wait({ tier: "global", timeoutMs: 60_000 });
    settleAll();
    await expect(wait).resolves.toBeUndefined();
    expect(signals[0]!.aborted).toBe(true);
  });

  it.each([-1, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 31])(
    "rejects timeoutMs %s before waiting, without inviting a retry",
    async (timeoutMs) => {
      const { handle, signals } = heldWaitClient();
      await expect(handle.wait({ tier: "global", timeoutMs })).rejects.toThrow(
        /timeoutMs.*do not retry it/s,
      );
      expect(signals).toHaveLength(0);
    },
  );

  it.each(["authentication expired", "invalid token", "account disabled"])(
    "ends the runtime's credential wait when nothing renews a refusal (%s)",
    (reason) => {
      const { refuse, onAuthFailure, endCredentialWait } = heldWaitClient();
      refuse(reason);
      expect(endCredentialWait).toHaveBeenCalledOnce();
      expect(onAuthFailure).toHaveBeenCalledOnce();
    },
  );

  it("leaves the credential wait to a renewer, except for a revocation", () => {
    const { client, refuse, endCredentialWait } = heldWaitClient();
    const renews = vi.fn(() => true);
    setCredentialRenewal(client, renews);
    refuse("authentication expired");
    expect(renews).toHaveBeenCalledWith("expired");
    expect(endCredentialWait).not.toHaveBeenCalled();
    // No new token fixes a revocation, renewer or not.
    refuse("session revoked");
    expect(endCredentialWait).toHaveBeenCalledOnce();
  });
});
