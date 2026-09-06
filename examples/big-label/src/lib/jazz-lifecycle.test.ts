import { describe, expect, it, vi } from "vitest";
import { JazzLifecycle } from "./jazz-lifecycle.js";

function account(id: string) {
  return { id, identity: { issuer: "better-auth", subject: id } } as never;
}

function client(events: string[], id: string) {
  return {
    shutdown: vi.fn(async ({ waitForSync }: { waitForSync?: boolean } = {}) =>
      events.push(`shutdown:${id}:${waitForSync}`),
    ),
  } as never;
}

describe("BigLabel account lifecycle", () => {
  it("normally disposes a selected client on provider unmount", async () => {
    const events: string[] = [];
    const current = client(events, "A");
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => account("A") } as never,
      async () => current,
      () => {},
    );

    await lifecycle.attach();
    await lifecycle.close();

    expect(events).toEqual(["shutdown:A:undefined"]);
    expect(current.shutdown).toHaveBeenCalledWith();
  });

  it("reopens the selected client after provider sign-out fails", async () => {
    const events: string[] = [];
    const selected = account("A");
    const lifecycle = new JazzLifecycle(
      { getLoggedIn: () => selected } as never,
      async (next) => {
        events.push(`open:${next.id}`);
        return client(events, next.id);
      },
      () => {},
    );

    await lifecycle.attach();
    await expect(
      lifecycle.transition(async () => {
        throw new Error("provider sign-out failed");
      }),
    ).rejects.toThrow("provider sign-out failed");

    expect(events).toEqual(["open:A", "shutdown:A:true", "open:A"]);
  });
});
