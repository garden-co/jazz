import { describe, expect, it, vi } from "vitest";
import { JazzLifecycle } from "../src/lib/jazz-lifecycle.js";

function account(subject: string) {
  return { id: subject, identity: { issuer: "test", subject } } as never;
}

function client(events: string[], subject: string) {
  return {
    shutdown: vi.fn(async ({ waitForSync }: { waitForSync?: boolean } = {}) =>
      events.push(`shutdown:${subject}:${waitForSync}`),
    ),
  } as never;
}

describe("RecordPlayer JazzLifecycle", () => {
  it("waits for the old account to sync before logging it out and opening a new subject", async () => {
    const events: string[] = [];
    let selected = account("a");
    const manager = {
      getLoggedIn: () => selected,
      logout: () => {
        events.push("logout");
        selected = undefined as never;
      },
    } as never;
    const lifecycle = new JazzLifecycle(
      manager,
      async (next) => {
        events.push(`open:${next.identity.subject}`);
        return client(events, next.identity.subject);
      },
      () => {},
    );
    await lifecycle.reconcile("a", "session-a", async () => {});
    await lifecycle.reconcile("b", "session-b", async () => {
      events.push("login:b");
      selected = account("b");
    });
    expect(events).toEqual(["open:a", "shutdown:a:true", "logout", "login:b", "open:b"]);
  });

  it("allows an explicit signup retry after enrollment fails", async () => {
    const events: string[] = [];
    let selected: ReturnType<typeof account> | undefined;
    const manager = { getLoggedIn: () => selected, logout: vi.fn() } as never;
    const lifecycle = new JazzLifecycle(
      manager,
      async (next) => {
        events.push(`open:${next.identity.subject}`);
        return client(events, next.identity.subject);
      },
      () => {},
    );
    await expect(
      lifecycle.reconcile("new-user", "signup-session", async () => {
        events.push("register");
        throw new Error("signup failed");
      }),
    ).rejects.toThrow("signup failed");
    await lifecycle.reconcile("new-user", "signup-session", async () => {
      events.push("register");
      selected = account("new-user");
    });
    expect(events).toEqual(["register", "register", "open:new-user"]);
  });

  it("does not open a superseded session after its enrollment resolves late", async () => {
    const events: string[] = [];
    let selected: ReturnType<typeof account> | undefined;
    let releaseB!: () => void;
    const bPending = new Promise<void>((resolve) => {
      releaseB = resolve;
    });
    const manager = { getLoggedIn: () => selected, logout: vi.fn() } as never;
    const lifecycle = new JazzLifecycle(
      manager,
      async (next) => {
        events.push(`open:${next.identity.subject}`);
        return client(events, next.identity.subject);
      },
      () => {},
    );
    const b = lifecycle.reconcile("b", "session-b", async () => {
      await bPending;
      selected = account("b");
    });
    const c = lifecycle.reconcile("c", "session-c", async () => {
      selected = account("c");
    });
    releaseB();
    await Promise.all([b, c]);
    expect(events).toEqual(["open:c"]);
  });

  it("disposes a client whose open finishes after a newer session is requested", async () => {
    const events: string[] = [];
    let selected = account("a");
    let releaseOpen!: () => void;
    const openA = new Promise<void>((resolve) => {
      releaseOpen = resolve;
    });
    const manager = {
      getLoggedIn: () => selected,
      logout: () => {
        selected = undefined as never;
      },
    } as never;
    const lifecycle = new JazzLifecycle(
      manager,
      async (next) => {
        if (next.identity.subject === "a") await openA;
        events.push(`open:${next.identity.subject}`);
        return client(events, next.identity.subject);
      },
      () => {},
    );
    const a = lifecycle.reconcile("a", "session-a", async () => {});
    await new Promise((resolve) => setTimeout(resolve, 0));
    const b = lifecycle.reconcile("b", "session-b", async () => {
      selected = account("b");
    });
    releaseOpen();
    await Promise.all([a, b]);
    expect(events).toEqual(["open:a", "shutdown:a:undefined", "open:b"]);
  });

  // #3840: React development mode mounts the provider, cleans it up and mounts
  // it again in the same tick. On a reload the session is already signed in,
  // so nothing re-runs the effect afterwards: the second mount must not reuse
  // the first mount's reconciliation, which the cleanup's close() undoes.
  it("reopens when a mount, cleanup and remount happen before the first open", async () => {
    const events: string[] = [];
    const published: unknown[] = [];
    const selected = account("a");
    const manager = { getLoggedIn: () => selected, logout: vi.fn() } as never;
    const lifecycle = new JazzLifecycle(
      manager,
      async (next) => {
        events.push(`open:${next.identity.subject}`);
        return client(events, next.identity.subject);
      },
      (next) => published.push(next),
    );
    const login = async () => {
      events.push("login:a");
    };
    const first = lifecycle.reconcile("a", "session-a", login);
    const closed = lifecycle.close();
    const second = lifecycle.reconcile("a", "session-a", login);
    await Promise.all([first, closed, second]);
    expect(published.at(-1)).toBeDefined();
    expect(lifecycle.isCurrent("a", "session-a")).toBe(true);
    expect(events.at(-1)).toBe("open:a");
  });
});
