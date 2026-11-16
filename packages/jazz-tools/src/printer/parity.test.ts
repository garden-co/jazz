/**
 * Parity between the TS DSL and the Rust writer's grammar: every policy of
 * the writer's corpus (`crates/jazz/fixtures/app-artifact/policy-corpus.json`,
 * written by `artifact_tests.rs`) and of every example app is printed as TS,
 * compiled by the DSL to readable JSON, and must load to the same engine
 * policies (and bundle id) as the original file.
 */
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { AppJson, PermissionsJson } from "./app-json.js";
import { printSchemaTs } from "./index.js";

const REPO = join(__dirname, "..", "..", "..", "..");
const CORPUS = join(REPO, "crates", "jazz", "fixtures", "app-artifact", "policy-corpus.json");

interface Case {
  name: string;
  app: string;
  permissions: string;
}

/** The TS `generate`'s compile step: schema.ts in `dir` → the two JSON texts. */
type CompileTs = (dir: string) => Promise<{ app: string; permissions: string }>;

async function loadCompiler(): Promise<CompileTs | undefined> {
  // TODO(ts-authoring-core): the loader's name once core is merged.
  const specifier = "../dev/schema-loader.js";
  try {
    const loader = (await import(specifier)) as { compileAppFiles?: CompileTs };
    return loader.compileAppFiles;
  } catch {
    return undefined;
  }
}

function cases(): Case[] {
  const out: Case[] = [];
  if (existsSync(CORPUS)) {
    const corpus = JSON.parse(readFileSync(CORPUS, "utf8")) as {
      app: string;
      cases: { name: string; permissions: PermissionsJson }[];
    };
    for (const c of corpus.cases) {
      out.push({ name: c.name, app: corpus.app, permissions: JSON.stringify(c.permissions) });
    }
  }
  const walk = (dir: string, depth: number) => {
    const permissions = join(dir, "jazz.permissions.json");
    if (existsSync(permissions)) {
      out.push({
        name: dir.slice(REPO.length + 1),
        app: readFileSync(join(dir, "jazz.app.json"), "utf8"),
        permissions: readFileSync(permissions, "utf8"),
      });
    }
    if (depth >= 3) return;
    for (const e of readdirSync(dir, { withFileTypes: true })) {
      if (e.isDirectory() && e.name !== "node_modules" && !e.name.startsWith(".")) {
        walk(join(dir, e.name), depth + 1);
      }
    }
  };
  walk(join(REPO, "examples"), 0);
  return out;
}

async function enginePolicies(app: string, permissions: string) {
  const napi = (await import("jazz-napi")) as unknown as {
    loadAppArtifact(
      app: string,
      permissions: string,
    ): {
      bundle: string;
      deployment: { permissions: unknown };
    };
  };
  const loaded = napi.loadAppArtifact(app, permissions);
  return { bundle: loaded.bundle, permissions: loaded.deployment.permissions };
}

const compile = await loadCompiler();

describe.skipIf(!compile)("printed TS compiles to the same engine policies", () => {
  for (const c of cases()) {
    it(c.name, async () => {
      const printed = printSchemaTs(JSON.parse(c.app) as AppJson, JSON.parse(c.permissions));
      expect(printed.unsupported, "forms the DSL can't express").toEqual([]);
      const dir = mkdtempSync(join(tmpdir(), "jazz-parity-"));
      try {
        writeFileSync(join(dir, "schema.ts"), printed.files["schema.ts"]!);
        const compiled = await compile!(dir);
        expect(await enginePolicies(compiled.app, compiled.permissions)).toEqual(
          await enginePolicies(c.app, c.permissions),
        );
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });
  }
});

describe("the writer's corpus", () => {
  it("prints without a form the DSL can't express", () => {
    for (const c of cases()) {
      const printed = printSchemaTs(JSON.parse(c.app) as AppJson, JSON.parse(c.permissions));
      expect(printed.unsupported, c.name).toEqual([]);
    }
  });
});
