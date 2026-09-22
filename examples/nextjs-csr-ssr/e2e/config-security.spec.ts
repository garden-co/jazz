import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import assert from "node:assert/strict";
import dotenv from "dotenv";
import ts from "typescript";

const EXAMPLE_PATH = "examples/nextjs-csr-ssr";
const PRIVILEGED_KEYS: Record<string, true> = {
  BACKEND_SECRET: true,
  JAZZ_BACKEND_SECRET: true,
  ADMIN_SECRET: true,
  JAZZ_ADMIN_SECRET: true,
};

function trackedExampleFiles(): { root: string; files: string[] } {
  const root = execFileSync("git", ["rev-parse", "--show-toplevel"], {
    encoding: "utf8",
  }).trim();
  const output = execFileSync(
    "git",
    ["-C", root, "ls-files", "-z", "--", `:(top)${EXAMPLE_PATH}`],
    { encoding: "utf8" },
  );
  const files = output.split("\0").filter(Boolean);
  assert.notEqual(root, "", "repository root must be discovered");
  assert.ok(files.length > 0, "tracked example selection must not be empty");
  assert.ok(
    files.includes(`${EXAMPLE_PATH}/next.config.ts`),
    "tracked example selection must include next.config.ts sentinel",
  );
  assert.ok(
    files.includes(`${EXAMPLE_PATH}/lib/jazz-server.ts`),
    "tracked example selection must include jazz-server.ts sentinel",
  );
  return { root, files };
}

function propertyName(property: ts.PropertyName): string | undefined {
  if (ts.isIdentifier(property) || ts.isStringLiteral(property) || ts.isNumericLiteral(property)) {
    return property.text;
  }
  return undefined;
}

function findNonEmptyPrivilegedLiterals(source: string): string[] {
  const file = ts.createSourceFile(
    "next.config.ts",
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const violations: string[] = [];
  function visit(node: ts.Node): void {
    if (ts.isPropertyAssignment(node)) {
      const name = propertyName(node.name);
      const value = node.initializer;
      if (
        (name === "backendSecret" || PRIVILEGED_KEYS[name ?? ""] === true) &&
        (ts.isStringLiteral(value) || ts.isNoSubstitutionTemplateLiteral(value)) &&
        value.text.length > 0
      ) {
        const position = file.getLineAndCharacterOfPosition(node.getStart(file));
        violations.push(`${name} at ${position.line + 1}:${position.character + 1}`);
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(file);
  return violations;
}

test("tracked configuration contains no privileged admission material", () => {
  const { root, files } = trackedExampleFiles();
  assert.ok(
    !files.includes(`${EXAMPLE_PATH}/.env`),
    "tracked .env must be absent because it can contain live credentials",
  );

  const exampleRoot = join(root, EXAMPLE_PATH);
  const envExample = `${EXAMPLE_PATH}/.env.example`;
  if (files.includes(envExample)) {
    const parsed = dotenv.parse(readFileSync(join(root, envExample), "utf8"));
    for (const key of [
      ...Object.keys(PRIVILEGED_KEYS),
      "NEXT_PUBLIC_JAZZ_APP_ID",
      "NEXT_PUBLIC_JAZZ_SERVER_URL",
    ]) {
      assert.ok(
        parsed[key] === undefined || parsed[key] === "",
        `${envExample} must keep ${key} absent or empty`,
      );
    }
  }

  const nextConfig = readFileSync(join(exampleRoot, "next.config.ts"), "utf8");
  assert.deepEqual(
    findNonEmptyPrivilegedLiterals(nextConfig),
    [],
    "next.config.ts must not contain fixed privileged values",
  );

  const serverModule = ts.createSourceFile(
    "lib/jazz-server.ts",
    readFileSync(join(exampleRoot, "lib/jazz-server.ts"), "utf8"),
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const hasSideEffectServerOnlyImport = serverModule.statements.some(
    (statement) =>
      ts.isImportDeclaration(statement) &&
      statement.importClause === undefined &&
      ts.isStringLiteral(statement.moduleSpecifier) &&
      statement.moduleSpecifier.text === "server-only",
  );
  assert.equal(
    hasSideEffectServerOnlyImport,
    true,
    "jazz-server.ts must use a side-effect server-only import",
  );
});
