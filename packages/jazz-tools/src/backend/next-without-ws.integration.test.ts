import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, memory } from "./index.js";

// #5055: next() on Vercel without the optional `ws` peer. Vercel's
// upgradeWebSocket() needs it, so every sync socket used to get a 500,
// logged only per request. Now createJazz says so once, at startup, and
// sockets get the HTTP stream marker, so clients switch to the stream.
vi.mock("ws", () => {
  throw Object.assign(new Error("Cannot find package 'ws' imported from jazz-tools"), {
    code: "ERR_MODULE_NOT_FOUND",
  });
});

const app = s.defineApp({ todos: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.always();
});

let previous: string | undefined;
beforeEach(() => {
  previous = process.env.VERCEL;
  process.env.VERCEL = "1";
});
afterEach(() => {
  if (previous === undefined) delete process.env.VERCEL;
  else process.env.VERCEL = previous;
  vi.restoreAllMocks();
});

it("answers sockets on Vercel with the HTTP stream marker when `ws` is missing, and says so once", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const jazz = await createJazz({ app, permissions, store: memory() }).ready();
  try {
    const socketUrl = `http://app.test/jazz/apps/${jazz.appId}/ws`;
    const { GET, POST } = jazz.next();

    // The upgrade itself, as Vercel hands it to the route.
    const upgrade = await GET(
      new Request(socketUrl, { headers: { upgrade: "websocket", connection: "Upgrade" } }),
    );
    expect(upgrade.status).toBe(426);
    expect(upgrade.headers.get("x-jazz-stream")).toBe("1");
    // The plain request a browser sends after its socket failed: the stream
    // at once, not "retry the socket".
    const probe = await POST(new Request(socketUrl, { method: "POST" }));
    expect(probe.status).toBe(426);
    expect(probe.headers.get("x-jazz-stream")).toBe("1");
    // The stream itself is served.
    const stream = await POST(
      new Request(`http://app.test/jazz/apps/${jazz.appId}/stream`, { method: "POST" }),
    );
    expect(stream.status).toBe(200);
    await stream.body?.cancel();

    // One clear error, naming the fix, however many sockets come.
    await GET(new Request(socketUrl, { headers: { upgrade: "websocket" } }));
    const logged = error.mock.calls.map((call) => call.join(" "));
    // (vitest wraps the mocked import's failure, so this reads "could not
    // load"; a package that isn't installed reads "needs the ws package:
    // npm install ws".)
    expect(logged.filter((line) => /the ws package/.test(line))).toEqual([
      expect.stringMatching(
        /^jazz: next\(\) on Vercel (needs|could not load) the ws package.*Without it, sync sockets can't open: clients sync over the HTTP stream instead\.$/s,
      ),
    ]);
  } finally {
    await jazz.close();
  }
});
