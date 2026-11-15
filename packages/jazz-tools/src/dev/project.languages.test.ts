import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../../tests/cli/fixtures.js";
import { generateForDir } from "./codegen-napi.js";
import { generateApp } from "./project.js";

const { createWorkspace, fileExists, writeAppFiles } = createCliFixtures(import.meta.url);

/**
 * `npx jazz-tools generate` picks languages and writes bindings through
 * crates/jazz-codegen's `project`, as the `jazz-tools` binary does
 * (crates/jazz-cli/tests/generate_languages.rs runs the binary on the same
 * directories): no language selection of its own.
 */
describe("generate picks the app's languages as the binary does", () => {
  const MARKERS: Record<string, string> = {
    "Package.swift": "// swift-tools-version:5.9\n",
    "build.gradle.kts": 'plugins { kotlin("jvm") }\n',
    "go.mod": "module example.com/app\n\ngo 1.22\n",
  };

  for (const [marker, text] of Object.entries(MARKERS)) {
    it(`writes no jazz.generated.ts for an app with only a ${marker}`, async () => {
      const { root } = await createWorkspace();
      const dir = join(root, "app");
      await writeAppFiles(dir);
      await writeFile(join(dir, marker), text);

      // The workspace's package.json is two levels up: not this app's.
      await expect(generateApp({ cwd: dir })).rejects.toThrow(
        "nothing beside jazz.app.json says which language to generate bindings in",
      );
      expect(await fileExists(join(dir, "jazz.generated.ts"))).toBe(false);
    });
  }

  it("assumes TS where nothing names a language, as running the npm CLI says", async () => {
    const { root } = await createWorkspace();
    const dir = join(root, "schema");
    await writeAppFiles(dir);

    const result = await generateApp({ cwd: dir });

    expect(result.bindings).toEqual(["jazz.generated.ts"]);
    expect(await fileExists(join(dir, "jazz.generated.ts"))).toBe(true);
  });

  it("writes the bytes the shared code writes, for a TS app", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);
    await writeFile(join(root, "Package.swift"), MARKERS["Package.swift"]!);

    const result = await generateApp({ cwd: root });

    expect(result.bindings).toEqual(["jazz.generated.ts"]);
    const written = await readFile(join(root, "jazz.generated.ts"), "utf8");
    // Running the shared code again finds them current.
    const again = await generateForDir(root, { check: true });
    expect(again.current).toEqual(["jazz.generated.ts"]);
    expect(again.written).toEqual([]);
    expect(await readFile(join(root, "jazz.generated.ts"), "utf8")).toBe(written);
  });
});
