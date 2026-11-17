import { afterEach, describe, expect, it } from "vitest";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import type { LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import type { JazzSession, JazzSessionSnapshot } from "./jazz-session.js";
import type { JazzClient } from "../web/registered-jazz-client.js";
import { startJazzClient } from "../client/index.js";

const sessions: JazzSession<JazzClient>[] = [];

afterEach(async () => {
  for (const session of sessions.splice(0)) await session.close().catch(() => {});
});

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}

/** A local-first loader that fails its first `failures` loads. */
function flakyLoader(failures: number): LocalFirstLoader {
  let calls = 0;
  return async () => {
    if (calls++ < failures) throw new Error("synthetic startup failure");
    return { createClient: createLocalFirstDb };
  };
}

function start(client: LocalFirstLoader): JazzSession<JazzClient> {
  const session = startJazzClient({
    serverUrl: null,
    store: memoryStore(),
    driver: { type: "memory" },
    client,
  });
  sessions.push(session);
  return session;
}

/** Resolves with the first snapshot whose status is one of `statuses`. */
function until(
  session: JazzSession<JazzClient>,
  ...statuses: JazzSessionSnapshot<JazzClient>["status"][]
): Promise<JazzSessionSnapshot<JazzClient>> {
  return new Promise((resolve) => {
    const check = () => {
      const snapshot = session.getSnapshot();
      if (!statuses.includes(snapshot.status)) return;
      stop?.();
      resolve(snapshot);
    };
    let stop: (() => void) | undefined;
    stop = session.subscribe(check);
    check();
  });
}

describe("startJazzClient", () => {
  it("returns the session at once, starting, and it becomes ready", async () => {
    const session = start(flakyLoader(0));
    const first = session.getSnapshot();
    expect(first.status).toBe("starting");
    expect(first.client).toBeUndefined();
    const ready = await until(session, "ready", "error");
    expect(ready.status).toBe("ready");
    expect(ready.client?.db).toBeDefined();
  }, 60_000);

  it("shows a failed start as error, keeps the session open, and retry() starts it", async () => {
    const session = start(flakyLoader(1));
    const failed = await until(session, "ready", "error");
    expect(failed.status).toBe("error");
    expect(failed.error?.message).toContain("synthetic startup failure");
    expect(failed.recovery).toBe("session");

    await session.retry();
    expect(session.getSnapshot().status).not.toBe("error");
    const ready = await until(session, "ready", "error");
    expect(ready.status).toBe("ready");
    expect(ready.error).toBeUndefined();
  }, 60_000);

  it("starts again when an observer retries as soon as the failure shows", async () => {
    const session = start(flakyLoader(1));
    const retried = new Promise<void>((resolve, reject) => {
      const stop = session.subscribe(() => {
        if (session.getSnapshot().status !== "error") return;
        stop();
        session.retry().then(resolve, reject);
      });
    });
    await retried;
    expect((await until(session, "ready", "error")).status).toBe("ready");
  }, 60_000);

  it("closes while starting", async () => {
    const session = start(flakyLoader(0));
    expect(session.getSnapshot().status).toBe("starting");
    await session.close();
    expect(session.getSnapshot().status).toBe("closed");
    await expect(session.retry()).rejects.toThrow("Jazz session is closed");
  }, 60_000);
});
