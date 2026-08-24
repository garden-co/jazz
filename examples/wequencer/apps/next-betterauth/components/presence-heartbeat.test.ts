import { afterEach, describe, expect, it, vi } from "vitest";
import { schedulePresenceHeartbeat } from "./presence-heartbeat";

const EXPECTED_HEARTBEAT_INTERVAL_MS = 5_000;

describe("schedulePresenceHeartbeat", () => {
  afterEach(() => vi.useRealTimers());

  it("publishes once after commit, then only on the exact five-second cadence", () => {
    vi.useFakeTimers();
    const publish = vi.fn();
    const stop = schedulePresenceHeartbeat(publish);

    expect(publish).not.toHaveBeenCalled();
    vi.advanceTimersByTime(0);
    expect(publish).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(EXPECTED_HEARTBEAT_INTERVAL_MS - 1);
    expect(publish).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    expect(publish).toHaveBeenCalledTimes(2);
    vi.advanceTimersByTime(EXPECTED_HEARTBEAT_INTERVAL_MS * 2);
    expect(publish).toHaveBeenCalledTimes(4);

    stop();
  });

  it("uses current presence without acknowledgement feedback and cleans up stale schedules", () => {
    vi.useFakeTimers();
    let presence = "pending";
    const publish = vi.fn(() => presence);
    const stop = schedulePresenceHeartbeat(publish);

    vi.advanceTimersByTime(0);
    expect(publish).toHaveBeenLastCalledWith();
    expect(publish).toHaveReturnedWith("pending");

    // An acknowledgement updates the value read by the next heartbeat; it is
    // not itself a reason to publish another row.
    presence = "acknowledged";
    expect(publish).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(EXPECTED_HEARTBEAT_INTERVAL_MS);
    expect(publish).toHaveBeenCalledTimes(2);
    expect(publish).toHaveReturnedWith("acknowledged");

    stop();
    vi.advanceTimersByTime(EXPECTED_HEARTBEAT_INTERVAL_MS * 2);
    expect(publish).toHaveBeenCalledTimes(2);
  });

  it("cancels Strict Mode's probe schedule before its deferred first publish", () => {
    vi.useFakeTimers();
    const publish = vi.fn();
    const stopProbe = schedulePresenceHeartbeat(publish);
    stopProbe();
    const stopCommitted = schedulePresenceHeartbeat(publish);

    vi.advanceTimersByTime(0);
    expect(publish).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(EXPECTED_HEARTBEAT_INTERVAL_MS);
    expect(publish).toHaveBeenCalledTimes(2);

    stopCommitted();
  });
});
