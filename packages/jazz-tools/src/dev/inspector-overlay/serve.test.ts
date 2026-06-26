import { describe, it, expect } from "vitest";
import { Module } from "node:module";
import { createOverlayHandler } from "./serve.js";

function fakeRes() {
  const headers: Record<string, string> = {};
  const state = { body: "", statusCode: 200 };
  return {
    res: {
      setHeader: (k: string, v: string) => (headers[k] = v),
      get statusCode() {
        return state.statusCode;
      },
      set statusCode(v: number) {
        state.statusCode = v;
      },
      end: (b?: string | Buffer) => (state.body = b ? b.toString() : ""),
    },
    headers,
    state,
  };
}

describe("overlay serve middleware", () => {
  it("ignores unrelated urls", async () => {
    const handler = createOverlayHandler({ appRoot: process.cwd() });
    expect(await handler({ url: "/index.html" }, fakeRes().res as never)).toBe(false);
  });
  it("404s embedded requests when jazz-inspector is not installed (no crash)", async () => {
    // A real consumer dev server has no ambient NODE_PATH, so resolving
    // `jazz-inspector` from a nonexistent app root fails. Vitest's forked worker,
    // however, injects NODE_PATH pointing at this monorepo's pnpm store (where
    // jazz-inspector is symlinked), which would otherwise make it resolvable from
    // any path. Neutralize that injection here to faithfully simulate "not installed".
    const savedNodePath = process.env.NODE_PATH;
    process.env.NODE_PATH = "";
    (Module as unknown as { _initPaths(): void })._initPaths();
    try {
      const handler = createOverlayHandler({ appRoot: "/nonexistent-app-root" });
      const r = fakeRes();
      expect(await handler({ url: "/__jazz/embedded/embedded.html" }, r.res as never)).toBe(true);
      expect(r.state.statusCode).toBe(404);
    } finally {
      process.env.NODE_PATH = savedNodePath;
      (Module as unknown as { _initPaths(): void })._initPaths();
    }
  });
});
