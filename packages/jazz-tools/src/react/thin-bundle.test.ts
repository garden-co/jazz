import { build, type Metafile } from "esbuild";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";

// The local-first client: its WASM loader, the runtime source that starts it,
// and the createDb that opens it. None of them may load before an upgrade.
const ENGINE = [
  "src/runtime/wasm-loader.ts",
  "src/runtime/default-runtime-source.ts",
  "src/runtime/default-create-db.ts",
];

const src = resolve(__dirname, "..");

async function bundle(entry: string): Promise<{ initial: Set<string>; all: Set<string> }> {
  const outdir = await mkdtemp(join(tmpdir(), "jazz-thin-bundle-"));
  try {
    const result = await build({
      stdin: { contents: entry, resolveDir: src, sourcefile: "app.tsx", loader: "tsx" },
      bundle: true,
      splitting: true,
      format: "esm",
      platform: "browser",
      outdir,
      write: false,
      metafile: true,
      logLevel: "silent",
      external: ["react", "react-dom", "react/jsx-runtime", "jazz-wasm", "jazz-napi", "node:*"],
      define: { "process.env.NODE_ENV": '"production"' },
    });
    return modules(result.metafile);
  } finally {
    await rm(outdir, { recursive: true, force: true });
  }
}

/** Source modules in the entry's static import closure, and in any chunk. */
function modules(metafile: Metafile) {
  const outputs = metafile.outputs;
  const entry = Object.keys(outputs).find((file) =>
    /<stdin>|app\.tsx$/.test(outputs[file]!.entryPoint ?? ""),
  )!;
  const initialFiles = new Set<string>();
  const pending = [entry];
  while (pending.length > 0) {
    const file = pending.pop()!;
    if (initialFiles.has(file)) continue;
    initialFiles.add(file);
    for (const imported of outputs[file]!.imports)
      if (imported.kind === "import-statement" && !imported.external) pending.push(imported.path);
  }
  const sources = (files: Iterable<string>) => {
    const found = new Set<string>();
    for (const file of files)
      for (const [input, { bytesInOutput }] of Object.entries(outputs[file]!.inputs))
        if (bytesInOutput > 0) found.add(input.replace(/^.*?(src\/)/, "$1"));
    return found;
  };
  return { initial: sources(initialFiles), all: sources(Object.keys(outputs)) };
}

describe("jazz-tools/react on the thin client", () => {
  it("keeps the local-first client out of the initial load", async () => {
    const { initial } = await bundle(`
      import { JazzProvider, useDb, useAll } from "./react/index.ts";
      console.log(JazzProvider, useDb, useAll);
    `);
    expect(initial.has("src/thin/thin-runtime.ts")).toBe(true);
    expect(ENGINE.filter((module) => initial.has(module))).toEqual([]);
  }, 60_000);

  it("loads the local-first client as its own chunk from the client prop", async () => {
    const { initial, all } = await bundle(`
      import { JazzProvider } from "./react/index.ts";
      console.log(JazzProvider, () => import("./local-first/index.ts"));
    `);
    expect(ENGINE.filter((module) => initial.has(module))).toEqual([]);
    expect(ENGINE.filter((module) => !all.has(module))).toEqual([]);
  }, 60_000);
});
