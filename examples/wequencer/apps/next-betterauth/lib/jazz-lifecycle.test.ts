import { describe, expect, it, vi } from "vitest";
import { JazzLifecycle } from "./jazz-lifecycle.js";

function account(id: string) {
  return { id, identity: { issuer: "better-auth", subject: id } } as never;
}

function client(events: string[], id: string, failShutdown = false) {
  return {
    shutdown: vi.fn(async ({ waitForSync }: { waitForSync?: boolean } = {}) => {
      events.push(`shutdown:${id}:${waitForSync}`);
      if (failShutdown) throw new Error(`sync failed:${id}`);
    }),
  };
}

describe("example-owned account lifecycle", () => {
  it("reopens the manager's actual selection after login fails", async () => {
    const events: string[] = [];
    let selected = account("A");
    const lifecycle = new JazzLifecycle(
      {
        getLoggedIn: () => selected,
        loginJWT: vi.fn(async () => {
          selected = account("B");
          throw new Error("token rejected");
        }),
      } as never,
      async (next) => {
        events.push(`open:${next.id}`);
        return client(events, next.id) as never;
      },
      () => {},
    );

    await lifecycle.attach();
    await expect(
      lifecycle.transition((manager) => manager.loginJWT({ getToken: async () => "jwt" })),
    ).rejects.toThrow("token rejected");

    expect(events).toEqual(["open:A", "shutdown:A:true", "open:B"]);
  });

  it("reopens the selected client when external sign-out rejects", async () => {
    const events: string[] = [];
    const selected = account("A");
    const published: string[] = [];
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => selected } as never,
      async (next) => {
        events.push(`open:${next.id}`);
        return client(events, next.id) as never;
      },
      (next) => published.push(next ? "client" : "none"),
    );

    await lifecycle.attach();
    await expect(
      lifecycle.transition(async () => {
        throw new Error("provider sign-out failed");
      }),
    ).rejects.toThrow("provider sign-out failed");

    expect(events).toEqual(["open:A", "shutdown:A:true", "open:A"]);
    expect(published).toEqual(["client", "none", "client"]);
  });

  it("keeps the old client published when sync shutdown fails", async () => {
    const events: string[] = [];
    const selected = account("A");
    const published: string[] = [];
    const current = client(events, "A", true);
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => selected, logout: vi.fn() } as never,
      async () => current as never,
      (next) => published.push(next ? "client" : "none"),
    );

    await lifecycle.attach();
    await expect(lifecycle.transition((manager) => manager.logout())).rejects.toThrow(
      "sync failed:A",
    );

    expect(events).toEqual(["shutdown:A:true"]);
    expect(current.shutdown).toHaveBeenCalledWith({ waitForSync: true });
    expect(published).toEqual(["client"]);
  });

  it("normally disposes a selected client when its provider unmounts", async () => {
    const events: string[] = [];
    const current = client(events, "A");
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => account("A") } as never,
      async () => current as never,
      () => {},
    );

    await lifecycle.attach();
    await lifecycle.close();

    expect(events).toEqual(["shutdown:A:undefined"]);
    expect(current.shutdown).toHaveBeenCalledOnce();
    expect(current.shutdown).toHaveBeenCalledWith();
  });

  it("uses ordinary close for a stale late open without requesting sync", async () => {
    const events: string[] = [];
    let current = true;
    let resolveOpen!: (value: ReturnType<typeof client>) => void;
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => account("A") } as never,
      () =>
        new Promise<never>((resolve) => {
          resolveOpen = (next) => resolve(next as never);
        }),
      (next) => events.push(next ? "publish" : "clear"),
    );

    const opening = lifecycle.attach(() => current);
    await vi.waitFor(() => expect(resolveOpen).toBeTypeOf("function"));
    current = false;
    const stale = {
      shutdown: vi.fn(async ({ waitForSync }: { waitForSync?: boolean } = {}) => {
        events.push(`shutdown:A:${waitForSync}`);
        if (waitForSync) throw new Error("stale client must not request sync");
      }),
    };
    resolveOpen(stale);
    await opening;

    expect(events).toEqual(["shutdown:A:undefined"]);
    expect(stale.shutdown).toHaveBeenCalledOnce();
    expect(stale.shutdown).toHaveBeenCalledWith();
  });
});
