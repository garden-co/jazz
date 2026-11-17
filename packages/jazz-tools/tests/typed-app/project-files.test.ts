import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createAppMigration, generateApp } from "../../src/dev/project.js";
import { checkRunningBindings } from "../../src/dev/app-files.js";

const SCHEMA = [
  'import { schema as s } from "jazz-tools";',
  "export const schema = s.defineSchema({",
  "  todos: s.table({ title: s.string(), done: s.boolean() }, {}),",
  "});",
  "export const permissions = schema.definePermissions({ todos: { read: true, insert: true } });",
  "",
].join("\n");

async function project(schema: string = SCHEMA): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "jazz-files-"));
  await writeFile(join(dir, "schema.ts"), schema);
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
    // Nothing generated yet: the files are stale before the bindings are.
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(
      /jazz\.app\.json and .*jazz\.permissions\.json are out of date with the app's TS/,
    );
    await generateApp({ cwd: dir });
    await rm(join(dir, "jazz.generated.ts"));
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(
      "no jazz.generated.ts: run `npx jazz-tools generate`",
    );
    await generateApp({ cwd: dir });
    await writeFile(join(dir, "jazz.generated.ts"), "// edited\n");
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(
      "jazz.generated.ts: not a module jazz-tools generated (its first line differs); regenerate it with `npx jazz-tools generate`",
    );
    expect(await readFile(join(dir, "jazz.generated.ts"), "utf8")).toBe("// edited\n");
  });

  it("refuses an edited schema without a migration, before touching the bindings", async () => {
    const dir = await project();
    await generateApp({ cwd: dir });
    const before = await readFile(join(dir, "jazz.generated.ts"), "utf8");
    const appBefore = await readFile(join(dir, "jazz.app.json"), "utf8");
    const permissionsBefore = await readFile(join(dir, "jazz.permissions.json"), "utf8");
    await writeFile(
      join(dir, "schema.ts"),
      SCHEMA.replace("done: s.boolean() }", "done: s.boolean(), note: s.string().optional() }"),
    );
    await expect(generateApp({ cwd: dir })).rejects.toThrow(
      "the schema changed since the last version: run `npx jazz-tools migrations create --name <name>`",
    );
    // Nothing is written when generate refuses: not the bindings, not jazz.app.json.
    expect(await readFile(join(dir, "jazz.generated.ts"), "utf8")).toBe(before);
    expect(await readFile(join(dir, "jazz.app.json"), "utf8")).toBe(appBefore);
    expect(await readFile(join(dir, "jazz.permissions.json"), "utf8")).toBe(permissionsBefore);

    const created = await createAppMigration({ cwd: dir, name: "add-note" });
    expect(created).toMatchObject({ changed: true, todos: [] });
    await generateApp({ cwd: dir });
    const app = JSON.parse(await readFile(join(dir, "jazz.app.json"), "utf8"));
    expect(app.history).toBeDefined();
    await expect(generateApp({ cwd: dir, check: true })).resolves.toBeDefined();
  });

  it("warns without permissions, and refuses a name-less migration", async () => {
    const dir = await project(SCHEMA.split("export const permissions")[0]);
    const result = await generateApp({ cwd: dir });
    expect(result.warnings.join("\n")).toMatch(/No permissions: .*every table denies everything/);
    await expect(createAppMigration({ cwd: dir, name: "" })).rejects.toThrow(
      "`jazz-tools migrations create` needs `--name <name>`: letters, digits, `-` and `_`, e.g. `--name add-due-date`.",
    );
  });

  it("points an alpha.65 project (jazz.app.json, no schema.ts) at upgrade", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-legacy-"));
    await writeFile(join(dir, "jazz.app.json"), '{ "format": 1, "tables": {} }\n');
    await expect(generateApp({ cwd: dir })).rejects.toThrow(/Run `npx jazz-tools upgrade`/);
  });
});

describe("createJazz's bindings check", () => {
  it("refuses bindings generated from another jazz.app.json, and passes without one", async () => {
    const dir = await project();
    await generateApp({ cwd: dir });
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

/** A crate using `jazz-app-support`: what makes a Cargo.toml a Rust app's, for the npm CLI as for the binary. */
const JAZZ_APP_CRATE = '[package]\nname = "app"\n\n[dependencies]\njazz-app-support = "0.1"\n';

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
    await expect(generateApp({ cwd: dir, check: true })).rejects.toThrow(/out of date/);
    expect((await generateApp({ cwd: dir })).bindings).toEqual([join("src", "jazz_app.rs")]);
    await expect(generateApp({ cwd: dir, check: true })).resolves.toMatchObject({ wrote: false });
    await expect(readFile(join(dir, "jazz.generated.ts"), "utf8")).rejects.toThrow();
  });
});

describe("generate beside a Cargo.toml that isn't a jazz-app-support crate", () => {
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
  it("refuses schema.ts in more than one place", async () => {
    const dir = await project();
    const { mkdir } = await import("node:fs/promises");
    await mkdir(join(dir, "src"));
    await writeFile(join(dir, "src", "schema.ts"), SCHEMA);
    await expect(generateApp({ cwd: dir })).rejects.toThrow(
      "found schema.ts in both schema.ts and src/schema.ts: keep one, or point the command at the app with --schema-dir.",
    );
  });
});
