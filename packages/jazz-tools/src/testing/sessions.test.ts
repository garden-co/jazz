import { describe, expect, it, vi } from "vitest";
import { closeJazzSessions } from "./sessions.js";
import { createStartedJazzSession, ownJazzSession } from "../session/jazz-session.js";
import { createJazzSessionOwner } from "../session/state.js";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import type { JWTAuth } from "../accounts/enrollment.js";

function openCore(shutdown: () => Promise<void>) {
  const account = { id: "a", identity: { issuer: "test", subject: "a" } } as AccountHandle;
  const accounts = new AccountManager<JWTAuth>(
    {
      createLocalFirst: () => account,
      restoreLocalFirst: () => account,
      logout: () => {},
      linkWithBackend: async () => account,
    },
    account,
  );
  return createJazzSessionOwner({
    accounts,
    openClient: async () => ({
      db: { onAuthChanged: () => () => {} },
      session: null,
      shutdown: vi.fn(shutdown),
    }),
  });
}

const start = (shutdown: () => Promise<void> = async () => {}) =>
  createStartedJazzSession({}, () => openCore(shutdown));

describe("closeJazzSessions", () => {
  it("closes every session still open, and shuts down their clients", async () => {
    const shutdown = vi.fn(async () => {});
    const a = await start(shutdown);
    const b = await start(shutdown);

    await closeJazzSessions();

    expect(a.getSnapshot().status).toBe("closed");
    expect(b.getSnapshot().status).toBe("closed");
    expect(shutdown).toHaveBeenCalledTimes(2);
  });

  it("closes a session that never started", async () => {
    const factory = vi.fn(() => openCore(async () => {}));
    const session = ownJazzSession({}, factory, { start: false });

    await closeJazzSessions();

    expect(session.getSnapshot().status).toBe("closed");
    expect(factory).not.toHaveBeenCalled();
  });

  it("forgets sessions the test closed itself", async () => {
    const shutdown = vi.fn(async () => {});
    const session = await start(shutdown);
    await session.close();

    await closeJazzSessions();

    expect(shutdown).toHaveBeenCalledTimes(1);
  });

  it("forgets sessions it closed, so the next test starts empty", async () => {
    const shutdown = vi.fn(async () => {});
    await start(shutdown);
    await closeJazzSessions();
    await closeJazzSessions();

    expect(shutdown).toHaveBeenCalledTimes(1);
  });

  it("closes the others when one fails to, then throws every failure", async () => {
    const ok = vi.fn(async () => {});
    const failing = await start(async () => {
      throw new Error("worker stuck");
    });
    const healthy = await start(ok);

    const error = await closeJazzSessions().then(
      () => undefined,
      (e: unknown) => e,
    );

    expect(error).toBeInstanceOf(AggregateError);
    expect((error as AggregateError).errors).toEqual([new Error("worker stuck")]);
    expect((error as AggregateError).message).toBe(
      "closeJazzSessions: 1 session(s) failed to close",
    );
    expect(ok).toHaveBeenCalledTimes(1);
    expect(healthy.getSnapshot().status).toBe("closed");
    expect(failing.getSnapshot().status).toBe("closed");
    await closeJazzSessions();
  });
});
