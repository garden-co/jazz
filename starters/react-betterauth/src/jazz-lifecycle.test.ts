import { expect, it, vi } from "vitest";
import { JazzLifecycle } from "./jazz-lifecycle";

it("reopens after React Strict Mode replays an effect cleanup", async () => {
  const events: string[] = [];
  const account = { id: "A", identity: { issuer: "better-auth", subject: "A" } } as never;
  const lifecycle = new JazzLifecycle(
    { getLoggedIn: () => account } as never,
    async () => ({ shutdown: vi.fn(async () => events.push("shutdown")) }) as never,
    (client) => events.push(client ? "publish" : "clear"),
  );

  const initialAttach = lifecycle.attach(async () => {});
  const cleanup = lifecycle.close();
  const replayedAttach = lifecycle.attach(async () => {});
  await Promise.all([initialAttach, cleanup, replayedAttach]);

  expect(events).toEqual(["publish", "shutdown", "clear", "publish"]);
});
