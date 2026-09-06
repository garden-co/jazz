import { describe, expect, it, vi } from "vitest";
import { JazzLifecycle } from "./jazz-lifecycle.js";

function account(id: string) {
  return { id, identity: { issuer: "better-auth", subject: id } } as never;
}

function client(events: string[], id: string, failSync = false) {
  return {
    shutdown: vi.fn(async ({ waitForSync }: { waitForSync?: boolean } = {}) => {
      events.push(`shutdown:${id}:${waitForSync}`);
      if (failSync && waitForSync) throw new Error("sync failed");
    }),
  } as never;
}

describe("provider-owned Jazz lifecycle", () => {
  it("reopens the selected handle after failed signup enrollment so retry can replace it", async () => {
    const events: string[] = [];
    let selected = account("old");
    const registerJWT = vi.fn(async () => {
      if (registerJWT.mock.calls.length === 1) {
        selected = account("partially-enrolled");
        throw new Error("enrollment failed");
      }
      selected = account("enrolled");
    });
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => selected, registerJWT } as never,
      async (next) => {
        events.push(`open:${next.id}`);
        return client(events, next.id);
      },
    );

    await lifecycle.attach(async () => {});
    await expect(
      lifecycle.transition((manager) => manager.registerJWT({ getToken: async () => "jwt" })),
    ).rejects.toThrow("enrollment failed");
    await lifecycle.transition((manager) => manager.registerJWT({ getToken: async () => "jwt" }));

    expect(events).toEqual([
      "open:old",
      "shutdown:old:true",
      "open:partially-enrolled",
      "shutdown:partially-enrolled:true",
      "open:enrolled",
    ]);
  });

  it("fences a stale provider account switch before it can publish its old selection", async () => {
    const events: string[] = [];
    let selected = account("A");
    let resolveLogin!: () => void;
    const loginJWT = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          resolveLogin = () => {
            selected = account("B");
            events.push("login:B");
            resolve();
          };
        }),
    );
    const manager = {
      getLoggedIn: () => selected,
      loginJWT,
      logout: () => {
        selected = undefined as never;
        events.push("logout");
      },
    } as never;
    const lifecycle = new JazzLifecycle(manager, async (next) => {
      events.push(`open:${next.id}`);
      return client(events, next.id);
    });
    let version = 1;

    await lifecycle.attach(async () => {});
    const staleSwitch = lifecycle.transition(
      (accounts) => accounts.loginJWT({ getToken: async () => "jwt" }),
      () => version === 1,
    );
    await vi.waitFor(() => expect(loginJWT).toHaveBeenCalledOnce());
    version = 2;
    const sessionLoss = lifecycle.transition(
      (accounts) => accounts.logout(),
      () => version === 2,
    );
    resolveLogin();
    await staleSwitch;
    await sessionLoss;

    expect(events).toEqual(["open:A", "shutdown:A:true", "login:B", "logout"]);
  });

  it("keeps the visible client usable when its sync shutdown rejects", async () => {
    const events: string[] = [];
    const selected = account("A");
    const visible = client(events, "A", true);
    const enroll = vi.fn();
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => selected } as never,
      async () => visible,
    );

    await lifecycle.attach(async () => {});
    await expect(lifecycle.transition(enroll)).rejects.toThrow("sync failed");

    expect(enroll).not.toHaveBeenCalled();
    expect(lifecycle.getClient()).toBe(visible);
    expect(events).toEqual(["shutdown:A:true"]);
  });

  it("disposes a client after a failed sync barrier without retrying that barrier", async () => {
    const events: string[] = [];
    const selected = account("A");
    const visible = client(events, "A", true);
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => selected } as never,
      async () => visible,
    );

    await lifecycle.attach(async () => {});
    await expect(lifecycle.transition(() => {})).rejects.toThrow("sync failed");
    await lifecycle.close();

    expect(lifecycle.getClient()).toBeUndefined();
    expect(events).toEqual(["shutdown:A:true", "shutdown:A:undefined"]);
  });

  it("does not reopen a retained A handle when external B login rejects", async () => {
    const events: string[] = [];
    const selected = account("A");
    const loginJWT = vi.fn(async () => {
      throw new Error("B admission failed");
    });
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => selected, loginJWT } as never,
      async (next) => {
        events.push(`open:${next.id}`);
        return client(events, next.id);
      },
    );

    await lifecycle.attach(async () => {});
    await expect(
      lifecycle.transition(
        (manager) => manager.loginJWT({ getToken: async () => "B-token" }),
        () => true,
        false,
      ),
    ).rejects.toThrow("B admission failed");
    await expect(
      lifecycle.transition(
        (manager) => manager.loginJWT({ getToken: async () => "B-token" }),
        () => true,
        false,
      ),
    ).rejects.toThrow("B admission failed");

    expect(events).toEqual(["open:A", "shutdown:A:true"]);
    expect(loginJWT).toHaveBeenCalledTimes(2);
    expect(lifecycle.getClient()).toBeUndefined();
  });

  it("disposes a deferred retry B client when the session changes to C before it opens", async () => {
    const events: string[] = [];
    let selected = account("A");
    let resolveB!: () => void;
    const lifecycle = new JazzLifecycle({ getLoggedIn: () => selected } as never, async (next) => {
      events.push(`open:${next.id}`);
      if (next.id === "B")
        await new Promise<void>((resolve) => {
          resolveB = resolve;
        });
      return client(events, next.id);
    });
    let version = 1;

    await lifecycle.attach(async () => {});
    const retryB = lifecycle.transition(
      () => {
        selected = account("B");
      },
      () => version === 1,
    );
    await vi.waitFor(() => expect(events).toContain("open:B"));
    version = 2;
    const switchC = lifecycle.transition(
      () => {
        selected = account("C");
      },
      () => version === 2,
    );
    resolveB();
    await retryB;
    await switchC;

    expect(events).toEqual([
      "open:A",
      "shutdown:A:true",
      "open:B",
      "shutdown:B:undefined",
      "open:C",
    ]);
  });
});
