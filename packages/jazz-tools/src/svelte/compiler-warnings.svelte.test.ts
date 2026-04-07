import { describe, expect, it } from "vitest";
import { compile, preprocess } from "svelte/compiler";
import { readFileSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { vitePreprocess } from "@sveltejs/vite-plugin-svelte";

const __dirname = dirname(fileURLToPath(import.meta.url));

async function compileAndWarn(filename: string) {
  const filepath = resolve(__dirname, filename);
  const source = readFileSync(filepath, "utf-8");
  const preprocessed = await preprocess(source, vitePreprocess(), {
    filename: filepath,
  });
  const result = compile(preprocessed.code, {
    filename: filepath,
    generate: "client",
  });
  return result.warnings;
}

describe("svelte components produce no state_referenced_locally warnings", () => {
  it("JazzSvelteProvider.svelte", async () => {
    const warnings = await compileAndWarn("JazzSvelteProvider.svelte");
    const bad = warnings.filter((w) => w.code === "state_referenced_locally");
    expect(bad, bad.map((w) => w.message).join("\n")).toEqual([]);
  });

  it("SyntheticUserSwitcher.svelte", async () => {
    const warnings = await compileAndWarn("SyntheticUserSwitcher.svelte");
    const bad = warnings.filter((w) => w.code === "state_referenced_locally");
    expect(bad, bad.map((w) => w.message).join("\n")).toEqual([]);
  });
});
