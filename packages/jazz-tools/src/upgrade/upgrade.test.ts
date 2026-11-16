/**
 * `jazz-tools upgrade` end to end, on fixture apps written for alpha.64's
 * DSL (read with the published alpha.63, which has the same surface).
 *
 * Golden tests compare every file of the converted app, and the printed
 * report, with `tests/upgrade/fixtures/<name>/expected/`. jazz.generated.ts
 * is jazz-codegen's output and has its own tests there, so only its
 * presence is checked. Regenerate the goldens with UPDATE_GOLDEN=1.
 */
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  countTypeErrors,
  formatReport,
  splitTypeErrors,
  upgradeProject,
  type UpgradeReport,
} from "./index.js";
import {
  copyFixture,
  FIXTURES,
  legacyModules,
  projectFiles,
  removeFixture,
  testGenerator,
} from "./upgrade-test-helpers.js";

let generate: ((appJson: string) => string) | undefined;
const scratch: string[] = [];

beforeAll(async () => {
  legacyModules();
  generate = await testGenerator();
}, 300_000);

afterEach(() => {
  for (const dir of scratch.splice(0)) removeFixture(dir);
});

function fixture(name: string): string {
  const dir = copyFixture(name);
  scratch.push(dir);
  return dir;
}

async function upgrade(dir: string, options: { dryRun?: boolean } = {}): Promise<UpgradeReport> {
  return upgradeProject({
    dir,
    skipTypecheck: true,
    dryRun: options.dryRun,
    generate:
      generate ??
      (() => {
        throw new Error(
          "no generator: set JAZZ_UPGRADE_TEST_GENERATOR or build jazz-napi with `generate`",
        );
      }),
  });
}

/** The report with scratch paths made stable. */
function printed(report: UpgradeReport, dir: string): string {
  return `${formatReport(report).split(dir).join("<app>")}\n`;
}

function expectGolden(name: string, dir: string, report: UpgradeReport): void {
  const files = projectFiles(dir);
  const generated = files["jazz.generated.ts"];
  delete files["jazz.generated.ts"];
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
  if (report.todos === 0) expect(generated, "jazz.generated.ts").toContain("bindApp");
  else expect(generated).toBeUndefined();
}

describe("jazz-tools upgrade: conversions", () => {
  it("turns s.computed and .transform into computed fields, transforms and jazz.computed.ts", async () => {
    const dir = fixture("computed");
    const report = await upgrade(dir);
    const app = JSON.parse(readFileSync(join(dir, "jazz.app.json"), "utf8"));
    const computed = app.tables.tasks.computed;
    // String-literal unions become enums; string, boolean and Date map directly.
    expect(computed.priority).toEqual({
      type: { enum: ["low", "medium", "high"] },
      from: ["score"],
    });
    expect(computed.label).toEqual({ type: "string", from: ["title", "score"] });
    expect(computed.done).toEqual({ type: "boolean", from: ["score"] });
    expect(computed.createdDay).toEqual({ type: "timestamp", from: ["id"] });
    // int vs float from the callback, a TODO where it can't tell.
    expect(computed.doubled.type).toBe("int");
    expect(computed.ratio.type).toBe("float");
    expect(computed.bounded.type.json).toMatch(/^TODO: /);
    expect(report.todos).toBe(1);
    // A named transform keeps its name; an inline one is named for its column.
    expect(app.tables.tasks.columns.dueAt).toEqual({ type: "timestamp", transform: "isoDate" });
    expect(app.tables.tasks.columns.tags).toEqual({ type: "string", transform: "tasksTags" });
    expect(app.transforms).toEqual({
      isoDate: { stored: "timestamp", view: "string" },
      tasksTags: { stored: "string", view: { json: { type: "array", items: { type: "string" } } } },
    });
    expect(app.tables.tasks.tsOnly).toBeUndefined();
    // The callbacks moved unchanged, with what they use.
    const moved = readFileSync(join(dir, "jazz.computed.ts"), "utf8");
    expect(moved).toContain(
      'priority: (row): Priority => (row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low"),',
    );
    expect(moved).toContain('import { clamp } from "./src/clamp";');
    expect(moved).toContain("} satisfies AppComputed;");
    expect(moved).toContain("} satisfies AppTransforms;");
    expectGolden("computed", dir, report);
  });

  it("checks the history and migrations are in jazz.app.json, deletes migrations/ and rewrites a backend", async () => {
    const dir = fixture("backend-history");
    const report = await upgrade(dir);
    expect(existsSync(join(dir, "migrations"))).toBe(false);
    expect(existsSync(join(dir, "schema.ts"))).toBe(false);
    expect(existsSync(join(dir, "permissions.ts"))).toBe(false);
    const server = readFileSync(join(dir, "server", "jazz.ts"), "utf8");
    expect(server).toContain('import { app as schemaApp, type Note } from "../jazz.generated.js";');
    expect(server).toContain(
      'import permissions from "../jazz.permissions.json" with { type: "json" };',
    );
    expect(server).toContain("app: schemaApp,\n  permissions,");
    expect(report.notes.join("\n")).toContain("2 schema versions and 1 migration");
    expect(report.notes.join("\n")).toMatch(/repeats 1 expression .*defs/);
    expectGolden("backend-history", dir, report);
  });

  it("drops slices, turns every slice into the app, and rewrites a Next app's @/ imports", async () => {
    const dir = fixture("slices-next");
    const report = await upgrade(dir);
    const app = JSON.parse(readFileSync(join(dir, "jazz.app.json"), "utf8"));
    expect(app.slices).toBeUndefined();
    const page = readFileSync(join(dir, "app", "page.tsx"), "utf8");
    expect(page).toContain(
      'import { app, app as boardsOnly, app as full, type Card, type CardStatus } from "@/jazz.generated";',
    );
    expect(page).toContain('import { MAX_ITEMS } from "@/app-extras";');
    expect(page).toContain("export const cards = full.cards.limit(MAX_ITEMS);");
    const backend = readFileSync(join(dir, "lib", "backend.ts"), "utf8");
    expect(backend).not.toContain("schema as s");
    expect(backend).toContain("export type Draft = CardInsert;");
    expect(report.rewrites.join("\n")).toContain(
      'full.slice("cards") → full (schema slicing is gone)',
    );
    // Inline module scripts in .html files are rewritten too.
    expect(readFileSync(join(dir, "app", "demo.html"), "utf8")).toContain(
      'import { app } from "../jazz.generated.js";',
    );
    expectGolden("slices-next", dir, report);
  });

  it("runs the app's own validate on its own binaries, whatever this process's Jazz overrides say", async () => {
    // As in the monorepo's sealed test runs, which point jazz-napi at this
    // release's build: inherited, alpha.63's validate would load it (or, here,
    // fail to load a binding that isn't there).
    const saved = { ...process.env };
    Object.assign(process.env, {
      JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
      JAZZ_CORRECTNESS_NAPI_BINDING: "/nonexistent/jazz-napi.node",
      JAZZ_CORRECTNESS_NAPI_FINGERPRINT: "0",
      NAPI_RS_NATIVE_LIBRARY_PATH: "/nonexistent/jazz-napi.node",
    });
    try {
      const dir = fixture("slices-next");
      const report = await upgrade(dir, { dryRun: true });
      expect(report.deleted).toContain("schema.ts");
    } finally {
      for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
      Object.assign(process.env, saved);
    }
  });

  it("counts an identical error the conversion adds to a file that already had one", () => {
    const key = "server/jazz.ts\0TS2322\0Type 'string' is not assignable to type 'number'.";
    const error = (line: number) => ({ key, text: `server/jazz.ts:${line}:14: TS2322 …` });
    const before = countTypeErrors([error(3)]);
    // The rewrite moved the old error to line 5 and added the same one on line 9.
    expect(splitTypeErrors(before, [error(5), error(9)])).toEqual({
      preexisting: ["server/jazz.ts:5:14: TS2322 …"],
      added: ["server/jazz.ts:9:14: TS2322 …"],
    });
    // Without errors before, every one counts; with none after, nothing does.
    expect(splitTypeErrors(undefined, [error(5)]).added).toHaveLength(1);
    expect(splitTypeErrors(before, [])).toEqual({ added: [], preexisting: [] });
  });

  it("reports type errors the project already had apart, and doesn't count them", async () => {
    const dir = fixture("backend-history");
    const server = join(dir, "server", "jazz.ts");
    writeFileSync(
      server,
      `${readFileSync(server, "utf8")}\nexport const planted: number = "not a number";\n`,
    );
    const report = await upgradeProject({ dir, generate });
    const planted = (errors: string[]) =>
      errors.filter((e) => e.includes("not assignable to type 'number'"));
    expect(planted(report.preexistingTypeErrors)).toHaveLength(1);
    expect(planted(report.preexistingTypeErrors)[0]).toMatch(/^server\/jazz\.ts:\d+:\d+: /);
    expect(planted(report.typeErrors)).toHaveLength(0);
    expect(formatReport(report)).toContain(
      "the project had before the conversion too (not upgrade's, and not counted)",
    );
  });

  it("converts a Better Auth app: its generated DSL tables go, its generation config and script follow", async () => {
    const dir = fixture("better-auth");
    const report = await upgrade(dir);
    expect(existsSync(join(dir, "schema-better-auth"))).toBe(false);
    const config = readFileSync(join(dir, "auth-generate.ts"), "utf8");
    expect(config).toContain("    schema: {},\n");
    expect(config).not.toContain('jazz-tools";');
    const scripts = JSON.parse(readFileSync(join(dir, "package.json"), "utf8")).scripts;
    expect(scripts["auth:generate"]).toBe(
      "npx @better-auth/cli generate --config ./auth-generate.ts --yes",
    );
    expect(scripts.build).toBe("jazz-tools generate && next build");
    expect(report.manual).toEqual([]);
    expectGolden("better-auth", dir, report);
  });

  it("writes nothing on a dry run", async () => {
    const dir = fixture("backend-history");
    const before = projectFiles(dir);
    const report = await upgrade(dir, { dryRun: true });
    expect(report.deleted).toContain("schema.ts");
    expect(projectFiles(dir)).toEqual(before);
  });
});

describe("jazz-tools upgrade: refusals", () => {
  async function refused(dir: string, message: RegExp): Promise<void> {
    const before = projectFiles(dir);
    await expect(upgrade(dir)).rejects.toThrow(message);
    // A refusal comes before the first write.
    expect(projectFiles(dir)).toEqual(before);
  }

  it("refuses transforms with dependsOn", async () => {
    await refused(
      fixture("depends-on"),
      /\.transform\(\{ dependsOn \}\) on items\.amount is not supported yet/,
    );
  });

  it("refuses a computed field reading another computed field", async () => {
    await refused(
      fixture("computed-chain"),
      /a computed field reading another computed field \(items\.quadrupled reads doubled\) is not supported yet/,
    );
  });

  it("refuses app files that are stale against schema.ts", async () => {
    const dir = fixture("backend-history");
    const schema = join(dir, "schema.ts");
    writeFileSync(
      schema,
      readFileSync(schema, "utf8").replace(
        "title: s.string(),",
        "title: s.string(), extra: s.string(),",
      ),
    );
    await refused(dir, /`validate --check` refused the app files/);
  });

  it("refuses permissions that read the environment (alpha.64 baked an unset value in as `true`)", async () => {
    await refused(
      fixture("env-permissions"),
      /read the environment[\s\S]*permissions\.ts:5: reads `process\.env\.UPGRADE_FIXTURE_ROOM`/,
    );
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
    execFileSync(
      process.execPath,
      [join(dir, "node_modules", "jazz-tools", "bin", "jazz-tools.js"), "validate"],
      {
        cwd: dir,
        stdio: "pipe",
      },
    );
    await refused(dir, /`true` inside anyOf\/allOf at tables\.messages\.read\.anyOf\[0\]/);
  });

  it("refuses an app whose schema.ts doesn't pass its migrations, which ships no history", async () => {
    const dir = fixture("backend-history");
    const schema = join(dir, "schema.ts");
    writeFileSync(
      schema,
      readFileSync(schema, "utf8")
        // Migrations that aren't the generated module: validate only warns.
        .replace(
          'import { migrations } from "./migrations/index.js";',
          "const migrations = { schemas: [], migrations: [] };",
        ),
    );
    // alpha.64's validate writes the files for it and only warns that the app ships none of its versions.
    execFileSync(
      process.execPath,
      [join(dir, "node_modules", "jazz-tools", "bin", "jazz-tools.js"), "validate"],
      {
        cwd: dir,
        stdio: "pipe",
      },
    );
    await refused(
      dir,
      /schema\.ts:\d+: schema\.ts doesn't pass its migrations module to s\.defineApp/,
    );
  });

  it("refuses enum columns with cases, which alpha.65 removed, naming each", async () => {
    await refused(
      fixture("enum-cases"),
      /column "issues\.kind" declares an enum whose cases carry fields, which Jazz no longer supports \(see https:\/\/github\.com\/garden-co\/jazz\/issues\/5758/,
    );
  });

  it("passes an enum with cases through when only an older version declares it (a store keeps every version)", async () => {
    const dir = fixture("enum-cases-history");
    const before = JSON.parse(readFileSync(join(dir, "jazz.app.json"), "utf8"));
    const report = await upgrade(dir);
    const after = JSON.parse(readFileSync(join(dir, "jazz.app.json"), "utf8"));
    // History and migrations as alpha.64 wrote them, the legacy column included.
    expect(after.history).toEqual(before.history);
    expect(after.migrations).toEqual(before.migrations);
    expect(JSON.stringify(after.history)).toContain('"enum":{"bug":{"severity":"int"}');
    expect(after.tables).toEqual(before.tables);
    expect(report.notes.join("\n")).toContain("2 schema versions and 1 migration");
    expect(existsSync(join(dir, "jazz.generated.ts"))).toBe(true);
  });

  it("refuses a project without jazz.app.json", async () => {
    const dir = fixture("backend-history");
    rmSync(join(dir, "jazz.app.json"));
    await refused(
      dir,
      /jazz\.app\.json is missing: run `npx jazz-tools validate` with jazz-tools 2\.0\.0-alpha\.63/,
    );
  });

  it("refuses when the app's jazz-tools has no DSL", async () => {
    const dir = fixture("backend-history");
    rmSync(join(dir, "node_modules"), { recursive: true, force: true });
    mkdirSync(join(dir, "node_modules", "jazz-tools"), { recursive: true });
    symlinkSync(
      join(legacyModules(), "typescript"),
      join(dir, "node_modules", "typescript"),
      "dir",
    );
    writeFileSync(
      join(dir, "node_modules", "jazz-tools", "package.json"),
      '{ "name": "jazz-tools", "version": "2.0.0-alpha.65" }',
    );
    await expect(upgrade(dir)).rejects.toThrow(
      /the app's jazz-tools is 2\.0\.0-alpha\.65\. Install jazz-tools@2\.0\.0-alpha\.64/,
    );
    expect(existsSync(join(dir, "schema.ts"))).toBe(true);
  });

  it("refuses an app that is already converted", async () => {
    const dir = fixture("backend-history");
    rmSync(join(dir, "schema.ts"));
    await expect(upgrade(dir)).rejects.toThrow(/already on files as source/);
  });
});
