import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AuthenticateExpiresAtError,
  DEFAULT_SESSION_TTL_SECONDS,
  MAX_TIMER_DELAY_MS,
  MIN_SESSION_MS,
  REAUTHENTICATE_LEAD_MS,
  reauthenticateAt,
  sessionDeadline,
  startTimer,
  withCheckedExpiry,
} from "./session-expiry.js";

afterEach(() => vi.useRealTimers());

describe("withCheckedExpiry", () => {
  it("passes a user with an expiresAt in seconds", () => {
    const user = { userId: "alice", expiresAt: 1_800_000_000 };
    expect(withCheckedExpiry(user)).toBe(user);
  });

  it("refuses a missing, non-numeric, non-finite or millisecond expiresAt", () => {
    for (const [expiresAt, says] of [
      [undefined, /returned none/],
      [null, /returned none/],
      ["1800000000", /returned 1800000000/],
      [Number.POSITIVE_INFINITY, /returned Infinity/],
      [1_800_000_000_000, /looks like milliseconds/],
    ] as const) {
      expect(() => withCheckedExpiry({ userId: "alice", expiresAt } as never)).toThrow(says);
      expect(() => withCheckedExpiry({ userId: "alice", expiresAt } as never)).toThrow(
        AuthenticateExpiresAtError,
      );
    }
  });
});

describe("reauthenticateAt", () => {
  const now = 1_000_000;

  it("asks a long session to re-authenticate the lead before it expires", () => {
    expect(reauthenticateAt(now, now + 3_600_000)).toBe(now + 3_600_000 - REAUTHENTICATE_LEAD_MS);
  });

  it("gives a short session half its length as lead, and never less than the minimum", () => {
    expect(reauthenticateAt(now, now + 20_000)).toBe(now + 10_000);
    expect(reauthenticateAt(now, now + 6_000)).toBe(now + MIN_SESSION_MS);
    // Shorter than the minimum: at its expiry, never past it.
    expect(reauthenticateAt(now, now + 2_000)).toBe(now + 2_000);
  });

  it("refuses an answer that has already expired", () => {
    expect(reauthenticateAt(now, now)).toBeNull();
    expect(reauthenticateAt(now, now - 1)).toBeNull();
  });
});

describe("startTimer", () => {
  it("waits out delays past what one timer takes, instead of firing at once", () => {
    vi.useFakeTimers();
    const run = vi.fn();
    // A 30-day session: one setTimeout would clamp this to 1 ms.
    const thirtyDays = 30 * 24 * 3600 * 1000;
    expect(thirtyDays).toBeGreaterThan(MAX_TIMER_DELAY_MS);
    startTimer(run, thirtyDays);
    vi.advanceTimersByTime(1);
    expect(run).not.toHaveBeenCalled();
    vi.advanceTimersByTime(MAX_TIMER_DELAY_MS);
    expect(run).not.toHaveBeenCalled();
    vi.advanceTimersByTime(thirtyDays - MAX_TIMER_DELAY_MS);
    expect(run).toHaveBeenCalledOnce();
  });

  it("never runs once cancelled, even across chained timers", () => {
    vi.useFakeTimers();
    const run = vi.fn();
    const cancel = startTimer(run, MAX_TIMER_DELAY_MS * 2);
    vi.advanceTimersByTime(MAX_TIMER_DELAY_MS + 1);
    cancel();
    vi.advanceTimersByTime(MAX_TIMER_DELAY_MS * 2);
    expect(run).not.toHaveBeenCalled();
  });
});

describe("sessionDeadline", () => {
  const now = 1_000_000_000_000;

  it("ends a session at its expiresAt when that comes before sessionTtlSeconds", () => {
    expect(sessionDeadline(now, now / 1000 + 600, 3600)).toBe(now + 600_000);
  });

  it("caps a long expiresAt at sessionTtlSeconds, one hour by default", () => {
    // A 30-day app session still re-authenticates within the hour.
    const thirtyDays = now / 1000 + 30 * 24 * 3600;
    expect(sessionDeadline(now, thirtyDays)).toBe(now + DEFAULT_SESSION_TTL_SECONDS * 1000);
    expect(sessionDeadline(now, thirtyDays, 300)).toBe(now + 300_000);
    // So the timer never needs more than one setTimeout.
    expect(sessionDeadline(now, thirtyDays) - now).toBeLessThan(MAX_TIMER_DELAY_MS);
  });
});
