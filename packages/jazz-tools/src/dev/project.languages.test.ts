import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../../tests/cli/fixtures.js";
import { createMigration, generate, langFlags } from "../cli.js";
import { generateForDir } from "./codegen-napi.js";
import { generateApp } from "./project.js";

const { createWorkspace, fileExists, writeAppFiles, captureConsoleLogs } = createCliFixtures(
  import.meta.url,
);

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

describe("npm CLI flags and messages from the shared code", () => {
  it("reads --lang in both forms and refuses one without a value", () => {
    expect(langFlags(["--lang", "rust", "--check", "--lang=ts"])).toEqual(["rust", "ts"]);
    expect(langFlags(["--check"])).toEqual([]);
    for (const args of [["--lang"], ["--lang", "--check"], ["--lang="]]) {
      expect(() => langFlags(args)).toThrow("Missing value for --lang");
    }
  });

  it("generates the languages --lang names, and refuses one that isn't available", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);
    await captureConsoleLogs(() => generate({ schemaDir: root, lang: ["rust"] }));
    expect(await fileExists(join(root, "src", "jazz_app.rs"))).toBe(true);
    expect(await fileExists(join(root, "jazz.generated.ts"))).toBe(false);
    // No back end for it: refused whichever languages are switched on.
    for (const lang of ["cobol"]) {
      await expect(generate({ schemaDir: root, lang: [lang] })).rejects.toThrow(
        /not (supported|available) yet/,
      );
    }
  });

  it("prints the shared code's note when only indexes changed", async () => {
    const { root } = await createWorkspace();
    const app = { format: 1, tables: { todos: { columns: { title: "string" } } } };
    await writeAppFiles(root, app, { format: 1, tables: { todos: { read: "true" } } });
    await captureConsoleLogs(() => generate({ schemaDir: root }));
    await writeAppFiles(
      root,
      { format: 1, tables: { todos: { columns: { title: "string" }, indexes: ["title"] } } },
      { format: 1, tables: { todos: { read: "true" } } },
    );
    const { result, logs } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, name: "index-title" }),
    );
    expect(result).toBeNull();
    expect(logs.join("\n")).toContain("Only single-column indexes changed: no migration needed");
  });
});

/** A Swift package depending on the Jazz Swift runtime: what makes it a Swift app's. */
const SWIFT_PACKAGE = `// swift-tools-version:5.9
import PackageDescription
let package = Package(
  name: "App",
  dependencies: [.package(url: "https://github.com/garden-co/jazz-swift", from: "2.0.0")]
)
`;

describe("generate in a Swift app", () => {
  // Runs once Swift is switched on in jazz-codegen (`Lang::ALL`); until then
  // the npm CLI, like the binary, has no Swift to generate.
  it("writes JazzApp.swift and no jazz.generated.ts, and --check passes on it", async (context) => {
    const { root } = await createWorkspace();
    const dir = join(root, "app");
    await writeAppFiles(dir);
    await writeFile(join(dir, "Package.swift"), SWIFT_PACKAGE);

    let result;
    try {
      result = await generateApp({ cwd: dir });
    } catch (error) {
      if (/says which language|not available yet/.test(String(error))) context.skip();
      throw error;
    }

    expect(result.bindings).toEqual(["JazzApp.swift"]);
    expect(await fileExists(join(dir, "JazzApp.swift"))).toBe(true);
    expect(await fileExists(join(dir, "jazz.generated.ts"))).toBe(false);
    await expect(generateApp({ cwd: dir, check: true })).resolves.toMatchObject({ wrote: false });
    await writeFile(join(dir, "JazzApp.swift"), "// edited\n");
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(/JazzApp\.swift/);
  });
});

const KOTLIN_BUILD = `plugins { kotlin("jvm") version "2.2.21" }
dependencies { implementation("tools.jazz:jazz-kotlin:2.0.0-alpha.66") }
`;

describe("generate in a Kotlin app", () => {
  it("writes src/main/kotlin/JazzApp.kt and no jazz.generated.ts, and --check passes on it", async () => {
    const { root } = await createWorkspace();
    const dir = join(root, "app");
    await writeAppFiles(dir);
    await writeFile(join(dir, "build.gradle.kts"), KOTLIN_BUILD);

    const result = await generateApp({ cwd: dir });

    const bindings = join("src", "main", "kotlin", "JazzApp.kt");
    expect(result.bindings).toEqual([bindings]);
    expect(await fileExists(join(dir, bindings))).toBe(true);
    expect(await fileExists(join(dir, "jazz.generated.ts"))).toBe(false);
    await expect(generateApp({ cwd: dir, check: true })).resolves.toMatchObject({ wrote: false });
    await writeFile(join(dir, bindings), "// edited\n");
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(/JazzApp\.kt/);
  });
});
