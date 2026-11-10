import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createJazz, memory, type Jazz } from "./create-jazz.js";

const upgrade = vi.hoisted(() => vi.fn(() => new Response("platform upgrade")));
vi.mock("@vercel/functions", () => ({ upgradeWebSocket: upgrade, getDeadline: () => undefined }));
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});
let jazz: Jazz | undefined;
afterEach(async () => {
  await jazz?.close();
  jazz = undefined;
  vi.unstubAllEnvs();
  upgrade.mockClear();
});

it("uses Vercel upgrades independently of the core provider and checks origins first", async () => {
  vi.stubEnv("VERCEL", "1");
  vi.stubEnv("VERCEL_ENV", "production");
  // The application runs on Vercel; its authority does not use vercel().
  jazz = await createJazz({ app, permissions, store: memory() }).ready();
  const url = `https://app.example/jazz/apps/${jazz.appId}/ws`;
  const request = (origin: string) =>
    new Request(url, { headers: { upgrade: "websocket", origin } });
  expect((await jazz.next().GET(request("https://other.example"))).status).toBe(403);
  expect(upgrade).not.toHaveBeenCalled();
  expect(await (await jazz.next().GET(request("https://app.example"))).text()).toBe(
    "platform upgrade",
  );
  expect(upgrade).toHaveBeenCalledOnce();
  // Only the Next adapter promises upgrades, not the ordinary Fetch handler.
  const probe = () => new Request(url, { method: "POST" });
  expect((await jazz.handle(probe())).headers.get("x-jazz-stream")).toBe("1");
  expect((await jazz.next().POST(probe())).headers.get("x-jazz-stream")).toBeNull();
});

it("does not hand an upgrade to the platform while its authority is unavailable", async () => {
  vi.stubEnv("VERCEL", "1");
  vi.stubEnv("VERCEL_ENV", "production");
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  try {
    jazz = createJazz({
      app,
      permissions,
      store: {
        open: async () => {
          throw new Error("core unavailable");
        },
      },
    });
    const response = await jazz.next().GET(
      new Request("https://app.example/jazz/apps/example/ws", {
        headers: { upgrade: "websocket" },
      }),
    );
    expect(response.status).toBe(503);
    expect(response.headers.get("retry-after")).toBe("1");
    expect(upgrade).not.toHaveBeenCalled();
  } finally {
    log.mockRestore();
  }
});
