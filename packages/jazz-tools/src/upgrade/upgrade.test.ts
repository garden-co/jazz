import { execFileSync } from "node:child_process";
import { legacyEnvironment } from "./fresh.js";
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
import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  statSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
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
  // Snapshots `generate` writes are stamped with the time: compare them by hash.
  const files = Object.fromEntries(
    Object.entries(projectFiles(dir)).map(([path, text]) => [
      path.replace(/(migrations\/snapshots\/)\d{8}T\d{6}-/, "$1STAMP-"),
      text,
    ]),
  );
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

/**
 * The app's own (alpha.65) `validate`, which writes its JSON files, on its
 * own binaries: as upgrade's check runs it, without this process's
 * JAZZ_CORRECTNESS_* / NAPI_RS_* overrides (CI's), which would run alpha.65
 * on this release's writer.
 */
function validateWithOwnJazzTools(
  dir: string,
  env: Record<string, string> = { UPGRADE_FIXTURE_ROOM: "lobby" },
): void {
  execFileSync(
    process.execPath,
    [join(dir, "node_modules", "jazz-tools", "bin", "jazz-tools.js"), "validate"],
    { cwd: dir, stdio: "pipe", env: { ...legacyEnvironment(process.env), ...env } },
  );
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

  it("expands a hand-written file's defs, one calling another, keeping the hashes and bundle id", async () => {
    const dir = fixture("chat");
    const file = join(dir, "jazz.permissions.json");
    const inlined = readFileSync(file, "utf8");
    const before = await identityOf(dir);
    // As an author would have factored it by hand in alpha.63–65.
    const withDefs = JSON.parse(
      inlined
        .replaceAll(
          "exists chatMembers(chatId = this.chatId, userId = session.user)",
          "member(this.chatId)",
        )
        .replaceAll(
          "exists chatMembers(chatId = this.id, userId = session.user)",
          "canManage(this.id)",
        ),
    );
    withDefs.defs = {
      "member($chat)": "exists chatMembers(chatId = $chat, userId = session.user)",
      "canManage($chat)": "member($chat)",
    };
    writeFileSync(file, `${JSON.stringify(withDefs, null, 2)}\n`);
    // alpha.65's own check runs too: the JSON isn't stale for having `defs`.
    const report = await upgrade(dir);
    expect(await identityOf(dir)).toEqual(before);
    expect(report.notes.join("\n")).toContain(
      "`defs` (member($chat), canManage($chat)) were expanded",
    );
    expect(readFileSync(join(dir, "schema.ts"), "utf8")).toContain(
      "const isChatMember = (chatId: s.Uuid) => exists.chatMembers({ chatId, userId: session.user });",
    );
  });

  it("writes a JSON-only app's TS (defs expanded, history as migrations/), keeping the hashes and bundle id", async () => {
    const dir = fixture("json-only");
    // The same app with its policies written out, as alpha.65 loaded the defs.
    const inlined = await artifactIdentity(
      readFileSync(join(dir, "jazz.app.json"), "utf8"),
      readFileSync(join(FIXTURES, "backend-history", "jazz.permissions.json"), "utf8"),
    );
    const report = await upgrade(dir);
    expect(await identityOf(dir)).toEqual(inlined);
    const schema = readFileSync(join(dir, "schema.ts"), "utf8");
    expect(schema).toContain("export const schema = s.defineSchema({");
    expect(schema).toContain("export const permissions = definePermissions({");
    expect(report.notes.join("\n")).toContain("`defs` (mine(), editable()) were expanded");
    expect(report.notes.join("\n")).toContain("are unchanged");
    const files = Object.keys(projectFiles(dir));
    expect(files.filter((f) => f.startsWith("migrations/snapshots/"))).toHaveLength(2);
    expect(files.filter((f) => /^migrations\/[^/]+\.ts$/.test(f))).toHaveLength(1);
    expect(files).toContain("src/jazz_app.rs");
  });

  it("writes a JSON-only app with transforms and computed fields, keeping the hashes and bundle id", async () => {
    const dir = fixture("json-only");
    // crates/jazz-app's tasks app: a transformed column (`isoDate`), computed fields, no permissions.
    const tasks = readFileSync(
      join(FIXTURES, "../../../../../crates/jazz-app/tests/fixtures/tasks/jazz.app.json"),
      "utf8",
    );
    writeFileSync(join(dir, "jazz.app.json"), tasks);
    rmSync(join(dir, "jazz.permissions.json"));
    const before = await artifactIdentity(
      tasks,
      '{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json", "tables": {} }\n',
    );
    await upgrade(dir);
    expect(await identityOf(dir)).toEqual(before);
    const schema = readFileSync(join(dir, "schema.ts"), "utf8");
    expect(schema).toContain('.transform("isoDate", s.string())');
    const app = JSON.parse(readFileSync(join(dir, "jazz.app.json"), "utf8"));
    expect(app.transforms).toEqual({ isoDate: { stored: "timestamp", view: "string" } });
    expect(Object.keys(app.tables.tasks.computed)).toEqual(["urgent", "titleLen"]);
  });

  it("reports the environment reads of the old permissions and marks them in the printed ones", async () => {
    const dir = fixture("env-permissions");
    // As set where the app last ran `validate`: the check runs it again.
    process.env.UPGRADE_FIXTURE_ROOM = "lobby";
    let report: UpgradeReport;
    try {
      report = await upgrade(dir);
    } finally {
      delete process.env.UPGRADE_FIXTURE_ROOM;
    }
    // Manual work, not a note.
    expect(report!.manual.join("\n")).toMatch(
      /permissions\.ts:\d+: reads `process\.env\.UPGRADE_FIXTURE_ROOM`: the printed permissions hold the value it had/,
    );
    expect(readFileSync(join(dir, "schema.ts"), "utf8")).toMatch(
      /\/\/ TODO: permissions\.ts:\d+ read `process\.env\.UPGRADE_FIXTURE_ROOM`/,
    );
  });

  it("puts a JSON-only app back byte for byte: its bindings, and no migrations/ it made", async () => {
    const dir = fixture("json-only");
    mkdirSync(join(dir, "src"));
    writeFileSync(join(dir, "src", "jazz_app.rs"), "// the bindings the app had\n");
    const before = projectFiles(dir);
    await expect(
      upgrade(dir, {
        generate: async (schemaDir) => {
          await testGenerate(schemaDir);
          throw new Error("planted failure after generate wrote everything");
        },
      }),
    ).rejects.toThrow(/`generate` failed on the printed TS, so upgrade put every file back/);
    expect(projectFiles(dir)).toEqual(before);
    expect(existsSync(join(dir, "migrations"))).toBe(false);
  });

  it("checks a TS app's defs without touching the rest of the app", async () => {
    const dir = fixture("chat");
    const file = join(dir, "jazz.permissions.json");
    const withDefs = JSON.parse(
      readFileSync(file, "utf8").replaceAll(
        "exists chatMembers(chatId = this.chatId, userId = session.user)",
        "member(this.chatId)",
      ),
    );
    withDefs.defs = {
      "member($chat)": "exists chatMembers(chatId = $chat, userId = session.user)",
    };
    writeFileSync(file, `${JSON.stringify(withDefs, null, 2)}\n`);
    mkdirSync(join(dir, "dist"));
    const built = join(dir, "dist", "app.js");
    writeFileSync(built, "// built\n");
    const old = new Date("2026-01-01T00:00:00Z");
    utimesSync(built, old, old);
    await upgrade(dir, { dryRun: true });
    expect(statSync(built).mtime).toEqual(old);
  });

  it("runs the app's own validate on its own binaries, whatever this process's Jazz overrides say", async () => {
    // As in the monorepo's sealed test runs, which point jazz-napi at this
    // release's build: inherited, alpha.65's validate would load it (or, here,
    // fail to load a binding that isn't there).
    const saved = { ...process.env };
    Object.assign(process.env, {
      JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
      JAZZ_CORRECTNESS_NAPI_BINDING: "/nonexistent/jazz-napi.node",
      JAZZ_CORRECTNESS_NAPI_FINGERPRINT: "0",
      NAPI_RS_NATIVE_LIBRARY_PATH: "/nonexistent/jazz-napi.node",
    });
    try {
      const report = await upgrade(fixture("chat"), { dryRun: true });
      expect(report.written).toContain("schema.ts");
    } finally {
      for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
      Object.assign(process.env, saved);
    }
  });

  it("reports type errors the project already had apart, and doesn't count them", async () => {
    const dir = fixture("backend-history");
    const server = join(dir, "server", "jazz.ts");
    writeFileSync(
      server,
      `${readFileSync(server, "utf8")}\nexport const planted: number = "not a number";\n`,
    );
    const report = await upgradeProject({ dir, generate: testGenerate });
    const planted = (errors: string[]) =>
      errors.filter((e) => e.includes("not assignable to type 'number'"));
    expect(planted(report.preexistingTypeErrors)).toHaveLength(1);
    expect(planted(report.preexistingTypeErrors)[0]).toMatch(/^server\/jazz\.ts:\d+:\d+: /);
    expect(planted(report.typeErrors)).toHaveLength(0);
    expect(formatReport(report)).toContain(
      "the project had before the conversion too (not upgrade's, and not counted)",
    );
  });

  it("types a printed helper's enum parameter as the enum, so the typecheck passes (adopter eval t3)", async () => {
    const dir = fixture("roles");
    const before = await identityOf(dir);
    const report = await upgradeProject({ dir, generate: testGenerate });
    expect(await identityOf(dir)).toEqual(before);
    const schema = readFileSync(join(dir, "schema.ts"), "utf8");
    expect(schema).toMatch(/role: "admin" \| "editor" \| "viewer"/);
    expect(schema).not.toContain('s.Value<"string">');
    // Errors the app had before (its own server deps) are reported apart; upgrade adds none.
    expect(report.typeErrors).toEqual([]);
  });

  it("renames a bare `import { schema }` from jazz-tools to `s` rather than refusing (adopter eval t6)", async () => {
    const dir = fixture("bare-schema");
    const before = await identityOf(dir);
    const report = await upgrade(dir);
    expect(await identityOf(dir)).toEqual(before);
    const schema = readFileSync(join(dir, "schema.ts"), "utf8");
    expect(schema).toContain('import { schema as s } from "jazz-tools";');
    expect(schema).toContain("export const schema = s.defineSchema(");
    expect(report.rewrites.join("\n")).toContain(
      "`import { schema }` from jazz-tools → `import { schema as s }`",
    );
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

  it("refuses `true` inside anyOf/allOf, the trace of a condition that compiled to nothing", async () => {
    const dir = fixture("env-permissions");
    const permissions = join(dir, "permissions.ts");
    writeFileSync(
      permissions,
      readFileSync(permissions, "utf8").replace(
        "process.env.UPGRADE_FIXTURE_ROOM!",
        "undefined as unknown as string",
      ),
    );
    validateWithOwnJazzTools(dir);
    await refused(dir, /`true` inside anyOf\/allOf at tables\.messages\.read\.anyOf\[0\]/);
  });

  it("refuses a bare `true` when the old permissions read the environment: it may be an unset value", async () => {
    const dir = fixture("env-permissions");
    const permissions = join(dir, "permissions.ts");
    writeFileSync(
      permissions,
      readFileSync(permissions, "utf8").replace(
        "policy.messages.allowRead.where(anyOf([{ room: ROOM }, { $createdBy: session.user }]))",
        "policy.messages.allowRead.where({ $createdBy: session.user })",
      ),
    );
    // Validated with the variable unset: `allowInsert.where({ room: ROOM })` became `"insert": "true"`.
    validateWithOwnJazzTools(dir, {});
    expect(
      JSON.parse(readFileSync(join(dir, "jazz.permissions.json"), "utf8")).tables.messages.insert,
    ).toBe("true");
    await refused(
      dir,
      /allows `messages\.insert` unconditionally[\s\S]*permissions\.ts:\d+: reads `process\.env\.UPGRADE_FIXTURE_ROOM`[\s\S]*Set those environment variables/,
    );
  });

  it("refuses enum columns with cases, which alpha.65 removed, naming each", async () => {
    await refused(
      fixture("enum-cases"),
      /column "issues\.kind" declares an enum whose cases carry fields, which Jazz no longer supports/,
    );
  });

  it("refuses computed columns and transforms, which alpha.66 doesn't convert yet", async () => {
    await refused(fixture("computed"), /computed columns and transforms is not supported yet/);
  });

  it("refuses an app whose schema.ts changed since its latest snapshot", async () => {
    const dir = fixture("backend-history");
    const schema = join(dir, "schema.ts");
    writeFileSync(
      schema,
      readFileSync(schema, "utf8").replace(
        "title: s.string(),",
        "title: s.string(), extra: s.string(),",
      ),
    );
    // alpha.65's validate writes the files, warning that the schema changed without a migration.
    validateWithOwnJazzTools(dir);
    await refused(dir, /schema\.ts changed since its latest snapshot/);
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
