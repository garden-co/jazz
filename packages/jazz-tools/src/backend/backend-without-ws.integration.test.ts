import { createServer } from "node:http";
import { expect, it, vi } from "vitest";

// `ws` is an optional peer of jazz-tools: an app that never upgrades
// sockets itself (a fetch handler, next(), a backend client) runs without
// it. Here `ws` can't be loaded at all: the backend entry still loads, a
// Jazz starts and answers, and only attach(), which upgrades sockets with
// `ws`, fails, naming the missing package. (A real Turbopack build
// without `ws` is in bundler-optional-peer.turbopack.test.ts.)
vi.mock("ws", () => {
  throw Object.assign(new Error("Cannot find package 'ws' imported from jazz-tools"), {
    code: "ERR_MODULE_NOT_FOUND",
  });
});

it("loads the backend entry and runs without ws; attach() names the missing package", async () => {
  const backend = await import("./index.js");
  const { schema: s } = await import("../index.js");
  // The Node client loads too.
  expect(typeof backend.createJazzClient).toBe("function");

  const app = s.defineApp({ todos: s.table({ title: s.string() }, {}) }, { migrations: "none" });
  const permissions = s.definePermissions(app, ({ policy }) => {
    policy.todos.allowRead.always();
  });
  const jazz = await backend.createJazz({ app, permissions, store: backend.memory() }).ready();
  try {
    // A host without socket upgrades sends clients to the HTTP stream.
    const probe = await jazz.handle(
      new Request(`http://app.test/jazz/apps/${jazz.appId}/ws`, { method: "POST" }),
    );
    expect(probe.status).toBe(426);
    expect(probe.headers.get("x-jazz-stream")).toBe("1");

    // vitest wraps the mocked import's failure, so this reads "could not
    // load"; a package that isn't installed reads "needs the ws package:
    // npm install ws" (optional-peer.test.ts).
    await expect(jazz.attach(createServer())).rejects.toThrow(
      /^attach\(\) (needs|could not load) the ws package/,
    );
  } finally {
    await jazz.close();
  }
});
