/**
 * Parity between the TS DSL and the Rust writer's grammar: every policy of
 * the writer's corpus (`crates/jazz/fixtures/app-artifact/policy-corpus.json`,
 * written by `artifact_tests.rs`) and of every example app is printed as TS,
 * compiled by the DSL to readable JSON, and must load to the same engine
 * policies (and bundle id) as the original file, with the same encryption
 * declarations: the schema hash covers only what is stored, so an encrypted
 * `bytes` column that lost `.encrypted()` would keep it.
 */
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { AppJson, PermissionsJson, PolicyJson } from "./app-json.js";
import { parseExpr, type Ast } from "./expr.js";
import { readablePermissions } from "../permissions/dsl.js";
import { readableSchema } from "../schema-dsl/define-schema.js";
import { printSchemaTs } from "./index.js";

const REPO = join(__dirname, "..", "..", "..", "..");
const CORPUS = join(REPO, "crates", "jazz", "fixtures", "app-artifact", "policy-corpus.json");
/** An app that declares encryption, with an encrypted `bytes` column (`files.content`). */
const ENCRYPTED = join(__dirname, "..", "..", "tests", "fixtures", "encrypted");

interface Case {
  name: string;
  app: string;
  permissions: string;
}

/**
 * The TS side: import the printed schema.ts (vitest resolves `jazz-tools` to
 * this package's source) and take the readable trees the DSL builds.
 */
async function compile(dir: string): Promise<{ app: string; permissions: string }> {
  const module = (await import(/* @vite-ignore */ join(dir, "schema.ts"))) as {
    schema: unknown;
    permissions: unknown;
  };
  const app = readableSchema(module.schema);
  const permissions = readablePermissions(module.permissions);
  if (!app) throw new Error("schema.ts's `schema` is not an `s.defineSchema(…)`");
  if (!permissions) throw new Error("schema.ts's `permissions` is not a `definePermissions(…)`");
  return { app: JSON.stringify({ format: 1, ...app }), permissions: JSON.stringify(permissions) };
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
  out.push(encryptedCase());
  return out;
}

function encryptedCase(): Case {
  return {
    name: "packages/jazz-tools/tests/fixtures/encrypted",
    app: readFileSync(join(ENCRYPTED, "jazz.app.json"), "utf8"),
    permissions: readFileSync(join(ENCRYPTED, "jazz.permissions.json"), "utf8"),
  };
}

/** Each table's encryption declaration, its `equality` list sorted: what the hash doesn't cover. */
function encryptionOf(app: string): Record<string, unknown> {
  const tables = (JSON.parse(app) as AppJson).tables;
  return Object.fromEntries(
    Object.entries(tables)
      .filter(([, table]) => table.encryption !== undefined)
      .map(([name, { encryption }]) => [
        name,
        {
          ...encryption,
          ...(encryption!.equality ? { equality: [...encryption!.equality].sort() } : {}),
        },
      ]),
  );
}

async function engine(app: string, permissions: string) {
  const napi = (await import("jazz-napi")) as unknown as {
    loadAppArtifact(
      app: string,
      permissions: string,
    ): { bundle: string; current: string; deployment: { permissions: unknown } };
  };
  const loaded = napi.loadAppArtifact(app, permissions);
  return {
    bundle: loaded.bundle,
    current: loaded.current,
    permissions: loaded.deployment.permissions,
  };
}

/** Every policy of a permissions file as parsed expressions, `anyOf`/`allOf` objects as `or`/`and`. */
function parsedPolicies(text: string): unknown {
  const policy = (p: PolicyJson): Ast => {
    if (typeof p === "string") return parseExpr(p);
    if ("anyOf" in p) return { kind: "or", items: p.anyOf.map(policy) };
    if ("allOf" in p) return { kind: "and", items: p.allOf.map(policy) };
    if ("where" in p) return policy(p.where);
    return { kind: "and", items: [policy(p.using), policy(p.withCheck)] };
  };
  const file = JSON.parse(text) as PermissionsJson;
  return Object.fromEntries(
    Object.entries(file.tables).map(([table, rules]) => [
      table,
      Object.fromEntries(
        Object.entries(rules).map(([op, rule]) => [
          op,
          typeof rule === "object" && "old" in rule
            ? { old: policy(rule.old), new: policy(rule.new) }
            : policy(rule as PolicyJson),
        ]),
      ),
    ]),
  );
}

let scratch = 0;

/**
 * Print `c` as TS (then `edit` the printed schema.ts), compile it, and
 * expect what the original loads to. Returns both current schema hashes
 * when the app loads.
 */
async function expectParity(
  c: Case,
  edit: (schemaTs: string) => string = (text) => text,
): Promise<{ original: string; compiled: string } | undefined> {
  const printed = printSchemaTs(JSON.parse(c.app) as AppJson, JSON.parse(c.permissions));
  expect(printed.unsupported, "forms the DSL can't express").toEqual([]);
  // Inside the package, so `jazz-tools` resolves to this source.
  const dir = join(__dirname, ".parity", String(scratch++));
  mkdirSync(dir, { recursive: true });
  try {
    // The schema namespace alone: the package root needs the built runtime.
    const namespace = join(__dirname, "..", "schema-namespace.ts");
    writeFileSync(
      join(dir, "schema.ts"),
      edit(printed.files["schema.ts"]!).replace(
        'from "jazz-tools"',
        `from ${JSON.stringify(namespace)}`,
      ),
    );
    const loads = await engine(c.app, c.permissions).then(
      () => true,
      () => false,
    );
    if (!loads) {
      // Grammar the writer prints but no deployment holds (unknown tables,
      // relations the engine refuses): the same expressions, parsed. A
      // table the app lacks can't be named in TS at all.
      const tables = Object.keys(JSON.parse(c.app).tables);
      const compiled = await compile(dir).catch((error: Error) => {
        const unknown = /from\.(\w+) is not a function/.exec(error.message)?.[1];
        if (unknown && !tables.includes(unknown)) return undefined;
        throw error;
      });
      if (!compiled) return undefined;
      expect(parsedPolicies(compiled.permissions)).toEqual(parsedPolicies(c.permissions));
      expect(encryptionOf(compiled.app), "encryption declarations").toEqual(encryptionOf(c.app));
      return undefined;
    }
    const compiled = await compile(dir);
    // The permissions, against the original app (whose history TS keeps elsewhere).
    expect(await engine(c.app, compiled.permissions)).toEqual(await engine(c.app, c.permissions));
    // The same current schema, loaded with its policies so deploy can validate space reads.
    const hashes = {
      original: (await engine(c.app, c.permissions)).current,
      compiled: (await engine(compiled.app, compiled.permissions)).current,
    };
    expect(hashes.compiled).toBe(hashes.original);
    // The same encryption declarations, which the hash doesn't cover.
    expect(encryptionOf(compiled.app), "encryption declarations").toEqual(encryptionOf(c.app));
    return hashes;
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

describe("printed TS compiles to the same engine policies", () => {
  for (const c of cases()) {
    it(c.name, async () => {
      await expectParity(c);
    });
  }

  it("catches an encrypted bytes column that lost .encrypted(), which keeps the schema hash", async () => {
    const c = encryptedCase();
    // files.content without its declaration: the stored column is bytes either way.
    const app = JSON.parse(c.app) as AppJson;
    const files = app.tables.files!;
    const lost = JSON.stringify({
      ...app,
      tables: {
        ...app.tables,
        files: { ...files, encryption: { ...files.encryption!, columns: {} } },
      },
    });
    expect((await engine(lost, c.permissions)).current).toBe(
      (await engine(c.app, c.permissions)).current,
    );
    const failure = await expectParity(c, (text) => {
      const edited = text.replace("content: s.bytes().encrypted(),", "content: s.bytes(),");
      expect(edited, "the printed files.content").not.toBe(text);
      return edited;
    }).then(
      () => undefined,
      (error: Error) => error,
    );
    expect(failure?.message).toMatch(/^encryption declarations/);
  });
});

describe("the writer's corpus", () => {
  it("prints without a form the DSL can't express", () => {
    for (const c of cases()) {
      const printed = printSchemaTs(JSON.parse(c.app) as AppJson, JSON.parse(c.permissions));
      expect(printed.unsupported, c.name).toEqual([]);
    }
  });
});
