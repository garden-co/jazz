import { describe, expect, it } from "vitest";
import {
  openPersistentWithRetry,
  OpfsInitRetryCancelled,
  OpfsInitRetryFailure,
  isRetryableOpfsInitError,
} from "./init-retry.js";

describe("openPersistentWithRetry", () => {
  it("retries retryable OPFS errors and eventually succeeds", async () => {
    let calls = 0;
    const result = await openPersistentWithRetry({
      open: async () => {
        calls += 1;
        if (calls < 3) {
          throw new Error("NoModificationAllowedError: createSyncAccessHandle conflict");
        }
        return "ok";
      },
      policy: {
        totalTimeoutMs: 2_000,
        baseDelayMs: 1,
        maxDelayMs: 2,
        jitterMs: 0,
      },
      now: (() => {
        let t = 0;
        return () => t;
      })(),
      sleep: async () => undefined,
      random: () => 0,
    });

    expect(result.value).toBe("ok");
    expect(result.attempts).toBe(3);
  });

  it("fails with timeout metadata when retryable errors exceed budget", async () => {
    let calls = 0;
    let nowMs = 0;

    try {
      await openPersistentWithRetry({
        open: async () => {
          calls += 1;
          throw new Error("NoModificationAllowedError: createSyncAccessHandle conflict");
        },
        policy: {
          totalTimeoutMs: 10,
          baseDelayMs: 5,
          maxDelayMs: 5,
          jitterMs: 0,
        },
        now: () => nowMs,
        sleep: async (ms) => {
          nowMs += ms;
        },
        random: () => 0,
      });
    } catch (error) {
      expect(error).toBeInstanceOf(OpfsInitRetryFailure);
      const typed = error as OpfsInitRetryFailure;
      expect(typed.timedOut).toBe(true);
      expect(typed.retryable).toBe(true);
      expect(typed.attempts).toBeGreaterThan(0);
    }
    expect(calls).toBeGreaterThan(1);
  });

  it("fails immediately on non-retryable init error", async () => {
    await expect(
      openPersistentWithRetry({
        open: async () => {
          throw new Error("SyntaxError: broken schema");
        },
        policy: {
          totalTimeoutMs: 100,
          baseDelayMs: 10,
          maxDelayMs: 10,
          jitterMs: 0,
        },
        sleep: async () => undefined,
      }),
    ).rejects.toMatchObject({
      name: "OpfsInitRetryFailure",
      retryable: false,
      timedOut: false,
      attempts: 1,
    });
  });

  it("cancels retries when cancellation signal is raised", async () => {
    let cancelled = false;

    await expect(
      openPersistentWithRetry({
        open: async () => {
          cancelled = true;
          throw new Error("NoModificationAllowedError: createSyncAccessHandle conflict");
        },
        isCancelled: () => cancelled,
        policy: {
          totalTimeoutMs: 100,
          baseDelayMs: 10,
          maxDelayMs: 10,
          jitterMs: 0,
        },
        sleep: async () => undefined,
      }),
    ).rejects.toBeInstanceOf(OpfsInitRetryCancelled);
  });
});

describe("isRetryableOpfsInitError", () => {
  it("matches known OPFS contention signatures", () => {
    expect(isRetryableOpfsInitError("NoModificationAllowedError: file locked")).toBe(true);
    expect(
      isRetryableOpfsInitError(
        "Failed to execute createSyncAccessHandle: Access Handles cannot be created if there is another open Access Handle",
      ),
    ).toBe(true);
    expect(isRetryableOpfsInitError("TypeError: invalid schema JSON")).toBe(false);
  });
});
