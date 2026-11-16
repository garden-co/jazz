import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createAppMigration, generateApp } from "../../src/dev/project.js";
import { checkRunningBindings } from "../../src/dev/app-files.js";

const APP = {
  format: 1,
  tables: { todos: { columns: { title: "string", done: "boolean" } } },
};
const PERMISSIONS = { format: 1, tables: { todos: { read: "true", insert: "true" } } };

async function project(app: unknown = APP, permissions: unknown = PERMISSIONS): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "jazz-files-"));
  await writeFile(join(dir, "jazz.app.json"), JSON.stringify(app, null, 2));
  if (permissions) await writeFile(join(dir, "jazz.permissions.json"), JSON.stringify(permissions));
  return dir;
}

describe("generate", () => {
  it("keeps reformatted bindings, which are still current", async () => {
    const dir = await project();
    await generateApp({ cwd: dir });
    const path = join(dir, "jazz.generated.ts");
    const reformatted = (await readFile(path, "utf8")).replace(/\n\n/g, "\n\n\n");
    await writeFile(path, reformatted);
    await expect(generateApp({ cwd: dir, check: true })).resolves.toMatchObject({ wrote: false });
    expect((await generateApp({ cwd: dir })).wrote).toBe(false);
    expect(await readFile(path, "utf8")).toBe(reformatted);
  });

  it("writes jazz.generated.ts, then finds it up to date", async () => {
    const dir = await project();
    expect((await generateApp({ cwd: dir })).wrote).toBe(true);
    const generated = await readFile(join(dir, "jazz.generated.ts"), "utf8");
    expect(generated).toContain("export const app = bindApp<Types>(");
    expect((await generateApp({ cwd: dir })).wrote).toBe(false);
    await expect(generateApp({ cwd: dir, check: true })).resolves.toMatchObject({ wrote: false });
  });

  it("--check refuses missing or stale bindings without writing", async () => {
    const dir = await project();
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(
      "no jazz.generated.ts: run `jazz-tools generate`",
    );
    await generateApp({ cwd: dir });
    await writeFile(join(dir, "jazz.generated.ts"), "// edited\n");
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(
      "jazz.generated.ts: not a module jazz-tools generated (its first line differs); regenerate it with `jazz-tools generate`",
    );
    expect(await readFile(join(dir, "jazz.generated.ts"), "utf8")).toBe("// edited\n");
  });

  it("refuses an edited schema without a migration, before touching the bindings", async () => {
    const dir = await project();
    await generateApp({ cwd: dir });
    const before = await readFile(join(dir, "jazz.generated.ts"), "utf8");
    const edited = {
      ...APP,
      tables: { todos: { columns: { title: "string", done: "boolean", note: "string?" } } },
    };
    await writeFile(join(dir, "jazz.app.json"), JSON.stringify(edited, null, 2));
    await expect(generateApp({ cwd: dir })).rejects.toThrow(
      "the schema changed since the last version: run `jazz-tools migrations create --name <name>`",
    );
    expect(await readFile(join(dir, "jazz.generated.ts"), "utf8")).toBe(before);

    const created = await createAppMigration({ cwd: dir, name: "add-note" });
    expect(created).toMatchObject({ changed: true, todos: [] });
    const app = JSON.parse(await readFile(join(dir, "jazz.app.json"), "utf8"));
    expect(app.history).toBeDefined();
    await expect(generateApp({ cwd: dir, check: true })).resolves.toBeDefined();
  });

  it("refuses a missing permissions file and a name-less migration", async () => {
    const dir = await project(APP, null);
    await expect(generateApp({ cwd: dir })).rejects.toThrow(
      /jazz\.permissions\.json is missing\. Every app has one, even if only backends read it: start from `\{ "format": 1, "tables": \{\} \}`, which denies everything\.$/,
    );
    await expect(createAppMigration({ cwd: dir, name: "" })).rejects.toThrow(
      "`jazz-tools migrations create` needs `--name <name>`: letters, digits, `-` and `_`, e.g. `--name add-due-date`.",
    );
  });

  it("points an alpha.65 project at upgrade", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-legacy-"));
    await writeFile(join(dir, "schema.ts"), "export {};\n");
    await expect(generateApp({ cwd: dir })).rejects.toThrow(
      "schema.ts is an alpha.65 schema: jazz.app.json is now the app's schema. Run `npx jazz-tools upgrade` to convert the app.",
    );
  });
});

describe("createJazz's bindings check", () => {
  it("refuses bindings generated from another jazz.app.json, and passes without one", async () => {
    const dir = await project();
    await expect(checkRunningBindings("sha256:" + "0".repeat(64), dir)).rejects.toThrow(
      "bindings out of date: jazz.generated.ts was generated from another jazz.app.json. Run `npx jazz-tools generate` and commit the jazz.generated.ts it writes.",
    );
    const empty = await mkdtemp(join(tmpdir(), "jazz-none-"));
    const warnings: string[] = [];
    await expect(
      checkRunningBindings("sha256:" + "0".repeat(64), empty, (message) => warnings.push(message)),
    ).resolves.toBeUndefined();
    expect(warnings).toEqual([expect.stringMatching(/Set JAZZ_APP_DIR/)]);
  });
});

/** A crate using `jazz-app`: what makes a Cargo.toml a Rust app's, for the npm CLI as for the binary. */
const JAZZ_APP_CRATE = '[package]\nname = "app"\n\n[dependencies]\njazz-app = "0.1"\n';

describe("generate in a Rust crate", () => {
  it("writes src/jazz_app.rs beside the TS bindings of a crate that has them", async () => {
    const dir = await project();
    await generateApp({ cwd: dir });
    await writeFile(join(dir, "Cargo.toml"), JAZZ_APP_CRATE);
    const result = await generateApp({ cwd: dir });
    expect(result.bindings).toEqual(["jazz.generated.ts", join("src", "jazz_app.rs")]);
    expect(await readFile(join(dir, "src", "jazz_app.rs"), "utf8")).toContain("jazz_app.rs");
    await expect(generateApp({ cwd: dir, check: true })).resolves.toMatchObject({ wrote: false });
  });

  it("gives a Rust-only crate only its Rust bindings, and --check passes on them", async () => {
    const dir = await project();
    await writeFile(join(dir, "Cargo.toml"), JAZZ_APP_CRATE);
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(/src.jazz_app\.rs/);
    expect((await generateApp({ cwd: dir })).bindings).toEqual([join("src", "jazz_app.rs")]);
    await expect(generateApp({ cwd: dir, check: true })).resolves.toMatchObject({ wrote: false });
    await expect(readFile(join(dir, "jazz.generated.ts"), "utf8")).rejects.toThrow();
  });
});

describe("generate beside a Cargo.toml that isn't a jazz-app crate", () => {
  it("writes only the TS bindings, as the binary does", async () => {
    const dir = await project();
    await generateApp({ cwd: dir });
    await writeFile(join(dir, "Cargo.toml"), '[workspace]\nmembers = ["crates/*"]\n');
    const result = await generateApp({ cwd: dir });
    expect(result.bindings).toEqual(["jazz.generated.ts"]);
    await expect(readFile(join(dir, "src", "jazz_app.rs"), "utf8")).rejects.toThrow();
  });
});

describe("finding the app's files", () => {
  it("refuses jazz.app.json in more than one place", async () => {
    const dir = await project();
    const { mkdir } = await import("node:fs/promises");
    await mkdir(join(dir, "src"));
    await writeFile(join(dir, "src", "jazz.app.json"), JSON.stringify(APP));
    await expect(generateApp({ cwd: dir })).rejects.toThrow(
      "found jazz.app.json in both jazz.app.json and src/jazz.app.json: keep one, or point the command at the app with --schema-dir.",
    );
  });
});
