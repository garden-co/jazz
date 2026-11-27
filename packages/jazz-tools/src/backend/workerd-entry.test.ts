import { existsSync, readFileSync } from "node:fs";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";

// What a Cloudflare Worker gets from `jazz-tools/backend`: wrangler picks the
// `workerd` condition, whose module must not reach jazz-napi (workerd can't
// load native addons) or the optional peers a Worker doesn't install.
// (The Durable Object tests bundle src/backend/workerd.ts directly.)
const pkg = JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8"));

describe("jazz-tools/backend under the workerd condition", () => {
  it("maps to the workerd entry and its types, built from src/backend/workerd.ts", () => {
    // `workerd` must come before Node's types: TypeScript and bundlers take
    // the first condition that matches (a top-level `types` would win).
    expect(pkg.exports["./backend"]).toEqual({
      workerd: { types: "./dist/backend/workerd.d.ts", default: "./dist/backend/workerd.js" },
      default: { types: "./dist/backend/index.d.ts", default: "./dist/backend/index.js" },
    });
    expect(Object.keys(pkg.exports["./backend"])[0]).toBe("workerd");
    expect(existsSync(new URL("./workerd.ts", import.meta.url))).toBe(true);
    // Its top-level wasm initialisation and engine install are side effects.
    expect(pkg.sideEffects).toContain("./dist/backend/workerd.js");
    expect(pkg.sideEffects).toContain("./dist/backend/node-native.js");
  });

  it("reaches neither jazz-napi nor an optional Node peer", async () => {
    const reached = new Set<string>();
    const staticImports = new Set<string>();
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
              if (args.kind === "import-statement") staticImports.add(args.path);
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
    // The engine is the backend build, never the browser build, which
    // leaves the backend out. (The client runtime's loader still names the
    // browser build in a dynamic import a backend never takes.)
    expect(staticImports).toContain("jazz-wasm/backend");
    expect([...staticImports].filter((path) => /^jazz-wasm(\/pkg|$)/.test(path))).toEqual([]);
    expect([...reached].filter((path) => path.startsWith("jazz-wasm/pkg"))).toEqual([]);
  });

  it("exports what the Node entry does, except the key-holding memoryPrivateStore", async () => {
    // Value exports of each entry, by esbuild's metafile (types aside: the
    // typecheck of an app's imports covers those).
    const exportsOf = async (entry: string) => {
      const result = await build({
        entryPoints: [new URL(entry, import.meta.url).pathname],
        bundle: true,
        write: false,
        metafile: true,
        outdir: "out",
        format: "esm",
        platform: "neutral",
        logLevel: "silent",
        plugins: [
          {
            name: "external",
            setup(b) {
              b.onResolve({ filter: /^[^./]/ }, (args) => ({ path: args.path, external: true }));
            },
          },
        ],
      });
      const [output] = Object.values(result.metafile.outputs).filter((o) => o.entryPoint);
      return new Set(output!.exports);
    };
    const node = await exportsOf("./index.ts");
    const workerd = await exportsOf("./workerd.ts");

    // A Worker holds no keys, so nothing to keep in a private store.
    const nodeOnly = ["memoryPrivateStore"];
    // The Durable Object surface only exists in a Worker.
    const workerdOnly = ["jazzBackend", "routeToJazzBackend", "serveJazz"];

    expect([...node].filter((name) => !workerd.has(name)).sort()).toEqual(nodeOnly);
    expect([...workerd].filter((name) => !node.has(name)).sort()).toEqual(workerdOnly);
    for (const name of ["keyless", "EncryptionError", "unreadableRows"]) {
      expect(workerd, name).toContain(name);
    }
  });
});
