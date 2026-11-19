import { existsSync, readFileSync } from "node:fs";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";

// What a Cloudflare Worker gets from `jazz-tools/backend`: wrangler picks the
// `workerd` condition, whose module must not reach jazz-napi (workerd can't
// load native addons) or the optional peers a Worker doesn't install.
// (The Durable Object tests bundle src/backend/workerd.ts directly.)
const pkg = JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8"));

describe("jazz-tools/backend under the workerd condition", () => {
  it("maps to the workerd entry, built from src/backend/workerd.ts", () => {
    expect(pkg.exports["./backend"].workerd).toBe("./dist/backend/workerd.js");
    expect(existsSync(new URL("./workerd.ts", import.meta.url))).toBe(true);
    // Its top-level wasm initialisation and engine install are side effects.
    expect(pkg.sideEffects).toContain("./dist/backend/workerd.js");
    expect(pkg.sideEffects).toContain("./dist/backend/node-native.js");
  });

  it("reaches neither jazz-napi nor an optional Node peer", async () => {
    const reached = new Set<string>();
    await build({
      entryPoints: [new URL("./workerd.ts", import.meta.url).pathname],
      bundle: true,
      write: false,
      format: "esm",
      platform: "neutral",
      conditions: ["workerd", "worker", "import"],
      mainFields: ["module", "main"],
      logLevel: "silent",
      plugins: [
        {
          name: "record",
          setup(b) {
            // Everything outside jazz-tools' own sources is recorded, not bundled.
            b.onResolve({ filter: /^[^./]/ }, (args) => {
              reached.add(args.path);
              return { path: args.path, external: true };
            });
          },
        },
      ],
    });
    for (const forbidden of [
      "jazz-napi",
      "ws",
      "@vercel/functions",
      "@vercel/functions/oidc",
      "@vercel/sandbox",
    ]) {
      expect([...reached], forbidden).not.toContain(forbidden);
    }
    expect(reached).toContain("jazz-wasm");
  });
});
