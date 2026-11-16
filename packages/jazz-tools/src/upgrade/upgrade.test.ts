/**
 * `jazz-tools upgrade` end to end, on alpha.65 apps (from the examples at
 * v2.0.0-alpha.65-rc.1, trimmed), whose JSON files the published
 * jazz-tools@2.0.0-alpha.65 checks are fresh.
 *
 * Golden tests compare every file of the converted app, and the printed
 * report, with `tests/upgrade/fixtures/<name>/expected/`. jazz.generated.ts
 * is jazz-codegen's output and has its own tests there, so only its
 * presence is checked. Regenerate the goldens with UPDATE_GOLDEN=1.
 */
import { existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  artifactIdentity,
  countTypeErrors,
  formatReport,
  splitTypeErrors,
  upgradeProject,
  type UpgradeOptions,
  type UpgradeReport,
} from "./index.js";
import {
  copyFixture,
  FIXTURES,
  legacyModules,
  projectFiles,
  removeFixture,
  testGenerate,
} from "./upgrade-test-helpers.js";

const scratch: string[] = [];

beforeAll(() => {
  legacyModules();
}, 300_000);

afterEach(() => {
  for (const dir of scratch.splice(0)) removeFixture(dir);
});

function fixture(name: string): string {
  const dir = copyFixture(name);
  scratch.push(dir);
  return dir;
}

async function upgrade(dir: string, options: Partial<UpgradeOptions> = {}): Promise<UpgradeReport> {
  return upgradeProject({ dir, skipTypecheck: true, generate: testGenerate, ...options });
}

/** The report with scratch paths made stable. */
function printed(report: UpgradeReport, dir: string): string {
  return `${formatReport(report).split(dir).join("<app>")}\n`;
}

function expectGolden(
  name: string,
  dir: string,
  report: UpgradeReport,
  options: { generated?: boolean } = {},
): void {
  const files = projectFiles(dir);
  const generated = files["jazz.generated.ts"];
  delete files["jazz.generated.ts"];
  // Rust bindings are jazz-codegen's output too.
  const rust = files["src/jazz_app.rs"];
  delete files["src/jazz_app.rs"];
  if (options.generated === false) expect(rust, "src/jazz_app.rs").toContain("jazz");
  files["REPORT.txt"] = printed(report, dir);
  const expectedDir = join(FIXTURES, name, "expected");
  if (process.env.UPDATE_GOLDEN) {
    rmSync(expectedDir, { recursive: true, force: true });
    for (const [path, text] of Object.entries(files)) {
      mkdirSync(dirname(join(expectedDir, path)), { recursive: true });
      writeFileSync(join(expectedDir, path), text);
    }
  }
  expect(Object.keys(files).sort()).toEqual(Object.keys(projectFiles(expectedDir)).sort());
  for (const [path, text] of Object.entries(files)) {
    expect(text, path).toBe(readFileSync(join(expectedDir, path), "utf8"));
  }
  if (options.generated !== false) expect(generated, "jazz.generated.ts").toBeDefined();
}

async function identityOf(dir: string) {
  return artifactIdentity(
    readFileSync(join(dir, "jazz.app.json"), "utf8"),
    readFileSync(join(dir, "jazz.permissions.json"), "utf8"),
  );
}

describe("jazz-tools upgrade: conversions", () => {
  it("converts chat-react: defineSchema, printed permissions with helpers, imports from jazz.generated", async () => {
    const dir = fixture("chat");
    const before = await identityOf(dir);
    const report = await upgrade(dir);
    expect(await identityOf(dir)).toEqual(before);
    const schema = readFileSync(join(dir, "schema.ts"), "utf8");
    expect(schema).toContain("export const schema = s.defineSchema({");
    expect(schema).toContain(
      "const isChatMember = (chatId: s.Uuid) => exists.chatMembers({ chatId, userId: session.user });",
    );
    expect(schema).toContain(
      "updateTo: (chat, old) => allOf(isChatMember(chat.id), { isPublic: old.isPublic }),",
    );
    expect(schema).not.toContain("defineApp");
    expect(existsSync(join(dir, "permissions.ts"))).toBe(false);
    expect(existsSync(join(dir, "migrations", "index.ts"))).toBe(false);
    expect(readFileSync(join(dir, "src", "messages.ts"), "utf8")).toContain(
      'from "../jazz.generated.js"',
    );
    expectGolden("chat", dir, report);
  });

  it("converts big-label: Better Auth's tables and permissions, a long permissions.ts, migrations kept", async () => {
    const dir = fixture("better-auth");
    const before = await identityOf(dir);
    const report = await upgrade(dir);
    expect(await identityOf(dir)).toEqual(before);
    const auth = readFileSync(join(dir, "schema-better-auth", "schema.ts"), "utf8");
    expect(auth).toContain("export const tables = {");
    expect(auth).toContain(
      "better_auth_user: { read: false, insert: false, update: false, delete: false },",
    );
    const permissions = readFileSync(join(dir, "permissions.ts"), "utf8");
    expect(permissions).toContain('import { schema } from "./schema";');
    expect(permissions).toContain("...betterAuthPermissions,");
    expect(
      existsSync(
        join(
          dir,
          "migrations",
          "20261104T192647-remove-better-auth-issuer-b6640bee5491-d986c626f864.ts",
        ),
      ),
    ).toBe(true);
    expectGolden("better-auth", dir, report);
  });

  it("converts session-claim checks to eq / isIn", async () => {
    const dir = fixture("claims");
    const report = await upgrade(dir);
    const schema = readFileSync(join(dir, "schema.ts"), "utf8");
    expect(schema).toContain('eq(session.claims.role, "admin")');
    expect(schema).toContain('isIn(session.claims.role, ["admin", "member"])');
    expectGolden("claims", dir, report);
  });

  it("converts a Rust app, writing its Rust bindings instead of jazz.generated.ts's", async () => {
    const dir = fixture("rust");
    const before = await identityOf(dir);
    const report = await upgrade(dir);
    expect(await identityOf(dir)).toEqual(before);
    expect(report.written).toContain("src/jazz_app.rs");
    expect(report.notes.join("\n")).toContain("run `cargo check`");
    expect(readFileSync(join(dir, "schema.ts"), "utf8")).toContain(
      "export const permissions = definePermissions({",
    );
    rmSync(join(dir, "jazz.generated.ts"), { force: true });
    expectGolden("rust", dir, report, { generated: false });
  });

  it("writes nothing on a dry run", async () => {
    const dir = fixture("chat");
    const before = projectFiles(dir);
    const report = await upgrade(dir, { dryRun: true });
    expect(report.written).toContain("schema.ts");
    expect(projectFiles(dir)).toEqual(before);
  });

  it("puts every file back when the converted app would deploy differently", async () => {
    const dir = fixture("chat");
    const before = projectFiles(dir);
    await expect(
      upgrade(dir, {
        generate: async (schemaDir) => {
          await testGenerate(schemaDir);
          const file = join(schemaDir, "jazz.permissions.json");
          writeFileSync(
            file,
            readFileSync(file, "utf8").replace('"read": "true"', '"read": "false"'),
          );
        },
      }),
    ).rejects.toThrow(/would deploy differently, so upgrade put every file back:\n {2}bundle id/);
    expect(projectFiles(dir)).toEqual(before);
  });

  it("counts an identical error the conversion adds to a file that already had one", () => {
    const key = "server/jazz.ts\0TS2322\0Type 'string' is not assignable to type 'number'.";
    const before = countTypeErrors([{ key }]);
    const split = splitTypeErrors(before, [
      { key, text: "server/jazz.ts:3:1: one" },
      { key, text: "server/jazz.ts:9:1: two" },
    ]);
    expect(split.preexisting).toEqual(["server/jazz.ts:3:1: one"]);
    expect(split.added).toEqual(["server/jazz.ts:9:1: two"]);
  });
});

describe("jazz-tools upgrade: refusals", () => {
  async function refused(dir: string, message: RegExp): Promise<void> {
    const before = projectFiles(dir);
    await expect(upgrade(dir)).rejects.toThrow(message);
    expect(projectFiles(dir)).toEqual(before);
  }

  it("refuses app files that are stale against schema.ts", async () => {
    const dir = fixture("chat");
    const schema = join(dir, "schema.ts");
    writeFileSync(
      schema,
      readFileSync(schema, "utf8").replace(
        "emoji: s.string(),",
        "emoji: s.string(), extra: s.string(),",
      ),
    );
    await refused(dir, /`validate --check` refused the app files/);
  });

  it("refuses a project without jazz.app.json", async () => {
    const dir = fixture("chat");
    rmSync(join(dir, "jazz.app.json"));
    await refused(dir, /jazz\.app\.json is missing/);
  });

  it("refuses when the app's jazz-tools is already alpha.66", async () => {
    const dir = fixture("chat");
    rmSync(join(dir, "node_modules"), { recursive: true, force: true });
    mkdirSync(join(dir, "node_modules", "jazz-tools"), { recursive: true });
    symlinkSync(
      join(legacyModules(), "typescript"),
      join(dir, "node_modules", "typescript"),
      "dir",
    );
    writeFileSync(
      join(dir, "node_modules", "jazz-tools", "package.json"),
      '{ "name": "jazz-tools", "version": "2.0.0-alpha.66" }',
    );
    await refused(
      dir,
      /the app's jazz-tools is 2\.0\.0-alpha\.66\. Install jazz-tools@2\.0\.0-alpha\.65/,
    );
  });

  it("refuses an app that is already converted", async () => {
    const dir = fixture("chat");
    const schema = join(dir, "schema.ts");
    writeFileSync(
      schema,
      readFileSync(schema, "utf8").replace(
        "const schema = {",
        "export const schema = s.defineSchema({",
      ),
    );
    await refused(dir, /already calls `defineSchema`: this app is on alpha\.66's layout/);
  });
});
