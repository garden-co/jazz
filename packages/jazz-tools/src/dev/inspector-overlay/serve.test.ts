import { describe, it, expect } from "vitest";
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
    const handler = createOverlayHandler();
    expect(await handler({ url: "/index.html" }, fakeRes().res as never)).toBe(false);
  });

  it("never serves files outside the embedded dir", async () => {
    // The embedded build resolves from jazz-tools' own dist (the inspector's
    // assets are bundled in), so there's no app-root input to attack — but a
    // crafted URL must still never escape the embedded dir.
    const handler = createOverlayHandler();
    const r = fakeRes();
    const handled = await handler(
      { url: "/__jazz/embedded/../../../../etc/passwd" },
      r.res as never,
    );
    // The handler owns the /__jazz/embedded prefix, so it handles the request...
    expect(handled).toBe(true);
    // ...but rejects the traversal (403 when the build is present, 404 when it
    // isn't) and never returns the out-of-tree file.
    expect([403, 404]).toContain(r.state.statusCode);
    expect(r.state.body).not.toContain("root:");
  });
});
