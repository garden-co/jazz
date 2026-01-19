import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { logger } from "../logger.js";
import { DeletedCoValuesEraserScheduler } from "../storage/DeletedCoValuesEraserScheduler.js";

describe("DeletedCoValuesEraserScheduler", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  test("scheduleStartupDrain runs once after startupDelayMs (when idle)", async () => {
    let runs = 0;
    const scheduler = new DeletedCoValuesEraserScheduler({
      run: async () => {
        runs += 1;
        return { hasMore: false };
      },
      opts: { throttleMs: 50, startupDelayMs: 10, followUpDelayMs: 10 },
    });

    scheduler.scheduleStartupDrain();

    expect(runs).toBe(0);
    await vi.advanceTimersByTimeAsync(10);
    expect(runs).toBe(1);
    scheduler.dispose();
  });

  test("onEnqueueDeletedCoValue is throttled (multiple enqueues -> one run)", async () => {
    let runs = 0;
    const scheduler = new DeletedCoValuesEraserScheduler({
      run: async () => {
        runs += 1;
        return { hasMore: false };
      },
      opts: { throttleMs: 30, startupDelayMs: 10, followUpDelayMs: 10 },
    });

    scheduler.onEnqueueDeletedCoValue();
    scheduler.onEnqueueDeletedCoValue();
    scheduler.onEnqueueDeletedCoValue();

    expect(runs).toBe(0);
    await vi.advanceTimersByTimeAsync(29);
    expect(runs).toBe(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(runs).toBe(1);

    // Ensure no second run was scheduled by repeated enqueues in the same throttle window.
    await vi.advanceTimersByTimeAsync(100);
    expect(runs).toBe(1);

    scheduler.dispose();
  });

  test("schedules follow-up phases while run reports hasMore=true", async () => {
    let remaining = 3;
    let runs = 0;
    const scheduler = new DeletedCoValuesEraserScheduler({
      run: async () => {
        runs += 1;
        remaining -= 1;
        return { hasMore: remaining > 0 };
      },
      opts: { throttleMs: 10, startupDelayMs: 10, followUpDelayMs: 10 },
    });

    scheduler.onEnqueueDeletedCoValue();

    await vi.runAllTimersAsync();
    expect(runs).toBe(3);
    scheduler.dispose();
  });

  test("never runs run concurrently (re-entrancy guard via internal state machine)", async () => {
    let concurrent = 0;
    let maxConcurrent = 0;
    let remaining = 2;

    const scheduler = new DeletedCoValuesEraserScheduler({
      run: async () => {
        concurrent += 1;
        maxConcurrent = Math.max(maxConcurrent, concurrent);

        await new Promise<void>((resolve) => setTimeout(resolve, 30));
        remaining -= 1;

        concurrent -= 1;
        return { hasMore: remaining > 0 };
      },
      opts: { throttleMs: 10, startupDelayMs: 10, followUpDelayMs: 10 },
    });

    scheduler.onEnqueueDeletedCoValue();
    await vi.advanceTimersByTimeAsync(10); // start first run

    // Even if we spam enqueues while active, they should be ignored.
    scheduler.onEnqueueDeletedCoValue();
    scheduler.onEnqueueDeletedCoValue();

    await vi.runAllTimersAsync();
    expect(remaining).toBe(0);
    expect(maxConcurrent).toBe(1);

    scheduler.dispose();
  });

  test("ignores enqueues while not idle, but schedules again once idle", async () => {
    let runs = 0;
    const scheduler = new DeletedCoValuesEraserScheduler({
      run: async () => {
        runs += 1;
        return { hasMore: false };
      },
      opts: { throttleMs: 30, startupDelayMs: 10, followUpDelayMs: 10 },
    });

    scheduler.onEnqueueDeletedCoValue(); // schedules first run
    await vi.advanceTimersByTimeAsync(5);
    scheduler.onEnqueueDeletedCoValue(); // should be ignored (not idle)

    await vi.advanceTimersByTimeAsync(25);
    expect(runs).toBe(1);

    // Now idle again; next enqueue should schedule another run.
    scheduler.onEnqueueDeletedCoValue();
    await vi.advanceTimersByTimeAsync(30);
    expect(runs).toBe(2);

    scheduler.dispose();
  });

  test("dispose cancels any scheduled run", async () => {
    let runs = 0;
    const scheduler = new DeletedCoValuesEraserScheduler({
      run: async () => {
        runs += 1;
        return { hasMore: false };
      },
      opts: { throttleMs: 30, startupDelayMs: 10, followUpDelayMs: 10 },
    });

    scheduler.onEnqueueDeletedCoValue();
    scheduler.dispose();

    await vi.advanceTimersByTimeAsync(60);
    expect(runs).toBe(0);
  });

  test("recovers when run throws (logs error and returns to idle so it can run again)", async () => {
    const err = new Error("boom");
    const errorSpy = vi.spyOn(logger, "error").mockImplementation(() => {});

    let runs = 0;
    const scheduler = new DeletedCoValuesEraserScheduler({
      run: async () => {
        runs += 1;
        if (runs === 1) throw err;
        return { hasMore: false };
      },
      opts: { throttleMs: 10, startupDelayMs: 10, followUpDelayMs: 10 },
    });

    scheduler.onEnqueueDeletedCoValue();
    await vi.advanceTimersByTimeAsync(10);
    expect(runs).toBe(1);

    expect(errorSpy).toHaveBeenCalledWith(
      "Error running deleted co values eraser scheduler",
      expect.objectContaining({ err }),
    );

    // If the scheduler didn't reset back to idle after the error, this enqueue would be ignored.
    scheduler.onEnqueueDeletedCoValue();
    await vi.advanceTimersByTimeAsync(10);
    expect(runs).toBe(2);

    scheduler.dispose();
    errorSpy.mockRestore();
  });
});
