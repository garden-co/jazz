import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { JazzUser } from "./create-jazz.js";
import {
  isOutsideRequestScope,
  isSameAdmission,
  RECHECK_DOCS_URL,
  RECHECK_FAILURES_BEFORE_END,
  recheckEvery,
  recheckFailedWarning,
} from "./session-recheck.js";

/** What Next.js throws for `cookies()` called outside a request (as of Next 15/16). */
const NEXT_SCOPE_ERROR = new Error(
  "`cookies` was called outside a request scope. Read more: https://nextjs.org/docs/messages/next-dynamic-api-wrong-context",
);

describe("isSameAdmission", () => {
  const admitted: JazzUser = { userId: "alice", claims: { role: "admin", org: { id: 1 } } };

  it("holds for the same user with the same claims, whatever their key order", () => {
    expect(
      isSameAdmission(admitted, { userId: "alice", claims: { org: { id: 1 }, role: "admin" } }),
    ).toBe(true);
  });

  it("counts no claims as empty claims", () => {
    expect(isSameAdmission({ userId: "alice" }, { userId: "alice", claims: {} })).toBe(true);
  });

  it.each([
    ["signed out", null],
    ["another user", { userId: "bob", claims: admitted.claims }],
    ["a changed claim", { userId: "alice", claims: { role: "viewer", org: { id: 1 } } }],
    ["a changed nested claim", { userId: "alice", claims: { role: "admin", org: { id: 2 } } }],
    ["a dropped claim", { userId: "alice", claims: { role: "admin" } }],
    ["an added claim", { userId: "alice", claims: { ...admitted.claims, beta: true } }],
  ] as const)("fails for %s", (_label, current) => {
    expect(isSameAdmission(admitted, current as JazzUser | null)).toBe(false);
  });

  it("fails when an array claim's order changes: arrays keep their order", () => {
    expect(
      isSameAdmission(
        { userId: "alice", claims: { groups: ["a", "b"] } },
        { userId: "alice", claims: { groups: ["b", "a"] } },
      ),
    ).toBe(false);
  });

  it("fails for claims that don't serialize, rather than throwing", () => {
    expect(isSameAdmission(admitted, { userId: "alice", claims: { big: 1n } })).toBe(false);
  });
});

describe("recheckEvery", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("refuses once, and stops, when a check no longer admits", async () => {
    let admitted = true;
    const check = vi.fn(async () => admitted);
    const refused = vi.fn();
    recheckEvery(1_000, check, refused);
    await vi.advanceTimersByTimeAsync(3_000);
    expect(check).toHaveBeenCalledTimes(3);
    expect(refused).not.toHaveBeenCalled();
    admitted = false;
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(check).toHaveBeenCalledTimes(4);
    expect(refused).toHaveBeenCalledOnce();
  });

  it("treats a failing check as no answer and asks again", async () => {
    const check = vi
      .fn<() => Promise<boolean>>()
      .mockRejectedValueOnce(new Error("auth database down"))
      .mockResolvedValue(false);
    const refused = vi.fn();
    recheckEvery(1_000, check, refused);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).toHaveBeenCalledOnce();
  });

  it(`ends the session as a refusal after ${RECHECK_FAILURES_BEFORE_END} failed checks in a row`, async () => {
    expect(RECHECK_FAILURES_BEFORE_END).toBe(3);
    const outage = new Error("auth database down");
    const check = vi.fn<() => Promise<boolean>>().mockRejectedValue(outage);
    const refused = vi.fn();
    const failed = vi.fn();
    recheckEvery(1_000, check, refused, { failed });
    await vi.advanceTimersByTimeAsync(2_000);
    expect(refused).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).toHaveBeenCalledOnce();
    expect(refused).toHaveBeenCalledWith({ kind: "failed", error: outage, inARow: 3 });
    expect(failed.mock.calls).toEqual([
      [outage, 1],
      [outage, 2],
      [outage, 3],
    ]);
    // It stopped: no more checks, no second refusal.
    await vi.advanceTimersByTimeAsync(10_000);
    expect(check).toHaveBeenCalledTimes(3);
    expect(refused).toHaveBeenCalledOnce();
  });

  it("counts only failures in a row: an answer starts the count again", async () => {
    const outage = new Error("auth database down");
    const check = vi
      .fn<() => Promise<boolean>>()
      .mockRejectedValueOnce(outage)
      .mockRejectedValueOnce(outage)
      .mockResolvedValueOnce(true)
      .mockRejectedValueOnce(outage)
      .mockRejectedValueOnce(outage)
      .mockResolvedValue(true);
    const refused = vi.fn();
    const failed = vi.fn();
    recheckEvery(1_000, check, refused, { failed });
    await vi.advanceTimersByTimeAsync(10_000);
    expect(refused).not.toHaveBeenCalled();
    expect(failed.mock.calls.map(([, inARow]) => inARow)).toEqual([1, 2, 1, 2]);
  });

  it("ends the session at the first failure endsAtOnce picks out", async () => {
    const outage = new Error("auth database down");
    const check = vi
      .fn<() => Promise<boolean>>()
      .mockRejectedValueOnce(outage)
      .mockRejectedValue(NEXT_SCOPE_ERROR);
    const refused = vi.fn();
    const failed = vi.fn();
    recheckEvery(1_000, check, refused, { failed, endsAtOnce: isOutsideRequestScope });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).toHaveBeenCalledExactlyOnceWith({
      kind: "failed",
      error: NEXT_SCOPE_ERROR,
      inARow: 2,
    });
    expect(failed).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(check).toHaveBeenCalledTimes(2);
  });

  it("ends the session when endsAtOnce itself throws, rather than keep it open", async () => {
    const refused = vi.fn();
    recheckEvery(1_000, () => Promise.reject(new Error("down")), refused, {
      endsAtOnce: () => {
        throw new Error("judge broke");
      },
    });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).toHaveBeenCalledOnce();
  });

  it("says a refusal is one", async () => {
    const refused = vi.fn();
    recheckEvery(1_000, () => false, refused);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).toHaveBeenCalledExactlyOnceWith({ kind: "refused" });
  });

  it("takes how many failures in a row end the session", async () => {
    const refused = vi.fn();
    recheckEvery(1_000, () => Promise.reject(new Error("down")), refused, { endAfter: 1 });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).toHaveBeenCalledOnce();
  });

  it.each([0, -1, 1.5, Number.NaN])("refuses endAfter %s", (endAfter) => {
    expect(() =>
      recheckEvery(
        1_000,
        () => true,
        () => {},
        { endAfter },
      ),
    ).toThrow(/failures\.endAfter must be a positive integer/);
  });

  it("still ends the session when the failure callback throws", async () => {
    const refused = vi.fn();
    recheckEvery(1_000, () => Promise.reject(new Error("down")), refused, {
      endAfter: 1,
      failed: () => {
        throw new Error("logger broke");
      },
    });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(refused).toHaveBeenCalledOnce();
  });

  it("reports no failure after it was stopped, even for a check already running", async () => {
    let fail!: (error: Error) => void;
    const refused = vi.fn();
    const failed = vi.fn();
    const stop = recheckEvery(
      1_000,
      () => new Promise<boolean>((_resolve, reject) => (fail = reject)),
      refused,
      { failed, endAfter: 1 },
    );
    await vi.advanceTimersByTimeAsync(1_000);
    stop();
    fail(new Error("down"));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(failed).not.toHaveBeenCalled();
    expect(refused).not.toHaveBeenCalled();
  });

  it("runs one check at a time", async () => {
    let answer!: (admitted: boolean) => void;
    const check = vi.fn(() => new Promise<boolean>((resolve) => (answer = resolve)));
    const refused = vi.fn();
    recheckEvery(1_000, check, refused);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(check).toHaveBeenCalledOnce();
    answer(false);
    await vi.advanceTimersByTimeAsync(0);
    expect(refused).toHaveBeenCalledOnce();
  });

  it("doesn't refuse after it was stopped, even for a check already running", async () => {
    let answer!: (admitted: boolean) => void;
    const refused = vi.fn();
    const stop = recheckEvery(
      1_000,
      () => new Promise<boolean>((resolve) => (answer = resolve)),
      refused,
    );
    await vi.advanceTimersByTimeAsync(1_000);
    stop();
    answer(false);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(refused).not.toHaveBeenCalled();
  });
});

// The check a signed-in user's sync session runs: authenticate, then compare.
describe("a signed-in user's recheck", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("ends the session once only the user's claims change", async () => {
    const admitted: JazzUser = { userId: "alice", claims: { role: "admin" } };
    let current: JazzUser = { userId: "alice", claims: { role: "admin" } };
    const authenticate = vi.fn(async () => current);
    const authenticateAgain = vi.fn();
    recheckEvery(
      60_000,
      async () => isSameAdmission(admitted, await authenticate()),
      authenticateAgain,
    );
    await vi.advanceTimersByTimeAsync(60_000);
    expect(authenticateAgain).not.toHaveBeenCalled();
    current = { userId: "alice", claims: { role: "viewer" } };
    await vi.advanceTimersByTimeAsync(60_000);
    expect(authenticateAgain).toHaveBeenCalledOnce();
  });
});

describe("isOutsideRequestScope", () => {
  it.each([
    ["cookies()", NEXT_SCOPE_ERROR],
    ["headers()", new Error("`headers` was called outside a request scope.")],
    ["a string", "`cookies` was called outside a request scope"],
  ])("recognises Next.js's error from %s", (_label, error) => {
    expect(isOutsideRequestScope(error)).toBe(true);
  });

  it.each([
    ["another error", new Error("auth database down")],
    ["no error", undefined],
    ["an object", { message: "called outside a request scope" }],
  ])("doesn't take %s for one", (_label, error) => {
    expect(isOutsideRequestScope(error)).toBe(false);
  });
});

describe("recheckFailedWarning", () => {
  it("names the request-scope cause, and the fix, for a request-scope error", () => {
    const warning = recheckFailedWarning(NEXT_SCOPE_ERROR);
    expect(warning).toContain("It threw a request-scope error");
    expect(warning).toContain("withAuth()");
    expect(warning).toContain("request.headers");
    expect(warning).toContain(RECHECK_DOCS_URL);
  });

  it("explains the re-check contract, without guessing a cause, for any other error", () => {
    const warning = recheckFailedWarning(new Error("auth database down"));
    expect(warning).not.toContain("It threw a request-scope error");
    expect(warning).toContain("outside your framework's request scope");
    expect(warning).toContain(`after ${RECHECK_FAILURES_BEFORE_END} failed re-checks in a row`);
    expect(warning).toContain(RECHECK_DOCS_URL);
  });
});
