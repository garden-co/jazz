import { describe, expect, it, vi } from "vitest";
import { JazzLifecycle } from "./jazz-lifecycle.js";

function account(id: string) {
  return { id, identity: { issuer: "test", subject: id } } as never;
}

function client(events: string[], id: string, failSync = false) {
  return {
    shutdown: vi.fn(async ({ waitForSync }: { waitForSync?: boolean } = {}) => {
      events.push(`shutdown:${id}:${waitForSync}`);
      if (failSync && waitForSync) throw new Error("sync failed");
    }),
  } as never;
}

describe("JazzLifecycle", () => {
  it("serializes a selected-handle replacement after the prior sync shutdown", async () => {
    const events: string[] = [];
    let selected = account("anonymous");
    const manager = { getLoggedIn: () => selected } as never;
    const lifecycle = new JazzLifecycle(manager, async (next) => {
      events.push(`open:${next.id}`);
      return client(events, next.id);
    });

    await lifecycle.attach(async () => {});
    await lifecycle.transition(() => {
      events.push("enroll");
      selected = account("member");
    });

    expect(events).toEqual(["open:anonymous", "shutdown:anonymous:true", "enroll", "open:member"]);
  });

  it("does not run the action or replace the visible context when sync shutdown fails", async () => {
    const events: string[] = [];
    const selected = account("anonymous");
    const manager = { getLoggedIn: () => selected } as never;
    const lifecycle = new JazzLifecycle(manager, async (next) => {
      events.push(`open:${next.id}`);
      return client(events, next.id, true);
    });

    await lifecycle.attach(async () => {});
    await expect(lifecycle.transition(() => events.push("enroll"))).rejects.toThrow("sync failed");
    expect(events).toEqual(["open:anonymous", "shutdown:anonymous:true"]);
  });

  it("reopens the retained local handle after a failed provider link", async () => {
    const events: string[] = [];
    const selected = account("anonymous");
    const linkJWT = vi.fn(async () => {
      events.push("link");
      throw new Error("link failed");
    });
    const manager = { getLoggedIn: () => selected, linkJWT } as never;
    const lifecycle = new JazzLifecycle(manager, async (next) => {
      events.push(`open:${next.id}`);
      return client(events, next.id);
    });

    await lifecycle.attach(async () => {});
    await expect(
      lifecycle.transition((accounts) => accounts.linkJWT({ getToken: async () => "token" })),
    ).rejects.toThrow("link failed");
    expect(linkJWT).toHaveBeenCalledOnce();
    expect(events).toEqual(["open:anonymous", "shutdown:anonymous:true", "link", "open:anonymous"]);
  });
});
