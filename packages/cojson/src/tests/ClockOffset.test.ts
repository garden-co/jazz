import { describe, expect, test } from "vitest";
import { ClockOffset } from "../ClockOffset.js";

function makeRng(seed: number) {
  let state = seed >>> 0;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return ((state >>> 0) / 0xffffffff) * 2 - 1;
  };
}

describe("ClockOffset", () => {
  test("currentOffset() returns 0 before any samples are ingested", () => {
    const clockOffset = new ClockOffset();

    expect(clockOffset.currentOffset()).toBe(0);
  });

  test("sampleCount() reports sliding window size capped at windowSize", () => {
    const windowSize = 4;
    const clockOffset = new ClockOffset({
      windowSize,
      // Keep the outlier gate wide enough to accept the spread of samples
      // we feed below; the point of this test is the window count, not
      // outlier rejection.
      outlierThresholdMs: 10_000,
    });

    expect(clockOffset.sampleCount()).toBe(0);

    for (let i = 0; i < 3; i++) {
      clockOffset.addSample({
        serverTime: 1_000 + i,
        localReceiveTime: 1_000,
      });
    }
    expect(clockOffset.sampleCount()).toBe(3);

    for (let i = 0; i < windowSize + 2; i++) {
      clockOffset.addSample({
        serverTime: 2_000 + i,
        localReceiveTime: 2_000,
      });
    }
    expect(clockOffset.sampleCount()).toBe(windowSize);
  });

  test("after a single sample, currentOffset() reflects serverTime - localReceiveTime", () => {
    const clockOffset = new ClockOffset();

    clockOffset.addSample({ serverTime: 10_500, localReceiveTime: 10_000 });

    expect(clockOffset.currentOffset()).toBe(500);
  });

  test("converges to within a small tolerance of the true offset under bounded jitter", () => {
    // True offset is 1000 ms; each sample has jitter of +/- 50 ms. The tolerance
    // of 60 ms is chosen to be slightly larger than the jitter amplitude so the
    // test does not assume a specific estimator (mean / median / trimmed mean
    // would all satisfy it), but is tight enough to prove convergence is real.
    const clockOffset = new ClockOffset();
    const rng = makeRng(42);
    const trueOffset = 1000;

    for (let i = 0; i < 64; i++) {
      const jitter = rng() * 50;
      const localReceiveTime = 1_000_000 + i * 100;
      const serverTime = localReceiveTime + trueOffset + jitter;
      clockOffset.addSample({ serverTime, localReceiveTime });
    }

    expect(clockOffset.currentOffset()).toBeGreaterThan(trueOffset - 60);
    expect(clockOffset.currentOffset()).toBeLessThan(trueOffset + 60);
  });

  test("a single huge outlier does not meaningfully move the estimate", () => {
    const clockOffset = new ClockOffset();
    const trueOffset = 1000;

    // Fill the window with clean samples near offset=1000.
    for (let i = 0; i < 32; i++) {
      const localReceiveTime = 2_000_000 + i * 100;
      clockOffset.addSample({
        serverTime: localReceiveTime + trueOffset,
        localReceiveTime,
      });
    }

    const before = clockOffset.currentOffset();

    // One extreme sample — offset would be ~100_000 if taken at face value.
    clockOffset.addSample({
      serverTime: 2_000_000 + 32 * 100 + 100_000,
      localReceiveTime: 2_000_000 + 32 * 100,
    });

    const after = clockOffset.currentOffset();

    expect(Math.abs(after - trueOffset)).toBeLessThan(50);
    expect(Math.abs(after - before)).toBeLessThan(50);
  });

  test("samples beyond maxAbsOffsetMs are ignored entirely", () => {
    const clockOffset = new ClockOffset({ maxAbsOffsetMs: 5000 });

    clockOffset.addSample({ serverTime: 10_200, localReceiveTime: 10_000 });
    const before = clockOffset.currentOffset();

    // Implied offset is 10_000_000 ms — well beyond the 5000 ms cap.
    clockOffset.addSample({
      serverTime: 20_000_000,
      localReceiveTime: 10_000_000,
    });

    expect(clockOffset.currentOffset()).toBe(before);
  });

  test("sliding window drops old samples once newer ones fill it", () => {
    const clockOffset = new ClockOffset({ windowSize: 10 });

    // Ten samples establishing offset ~500.
    for (let i = 0; i < 10; i++) {
      const localReceiveTime = 3_000_000 + i * 100;
      clockOffset.addSample({
        serverTime: localReceiveTime + 500,
        localReceiveTime,
      });
    }

    // Twenty samples at offset ~2000 — more than the window, so the old
    // offset=500 samples should be fully evicted.
    for (let i = 0; i < 20; i++) {
      const localReceiveTime = 3_100_000 + i * 100;
      clockOffset.addSample({
        serverTime: localReceiveTime + 2000,
        localReceiveTime,
      });
    }

    expect(clockOffset.currentOffset()).toBeGreaterThan(1950);
    expect(clockOffset.currentOffset()).toBeLessThan(2050);
  });

  test("a hostile peer's single bad sample does not poison later estimates", () => {
    const clockOffset = new ClockOffset();
    const trueOffset = 1000;

    // Warm-up with good samples.
    for (let i = 0; i < 16; i++) {
      const localReceiveTime = 4_000_000 + i * 100;
      clockOffset.addSample({
        serverTime: localReceiveTime + trueOffset,
        localReceiveTime,
      });
    }

    // Hostile sample — way off.
    clockOffset.addSample({
      serverTime: 4_000_000 + 16 * 100 + 500_000,
      localReceiveTime: 4_000_000 + 16 * 100,
    });

    // More good samples after the attack.
    for (let i = 17; i < 48; i++) {
      const localReceiveTime = 4_000_000 + i * 100;
      clockOffset.addSample({
        serverTime: localReceiveTime + trueOffset,
        localReceiveTime,
      });
    }

    expect(clockOffset.currentOffset()).toBeGreaterThan(trueOffset - 25);
    expect(clockOffset.currentOffset()).toBeLessThan(trueOffset + 25);
  });
});
