import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";
import { getCollectedSchema, resetCollectedState } from "../../src/dsl.js";
import { schemaToWasm } from "../../src/codegen/schema-reader.js";
import { generateTypes } from "../../src/codegen/type-generator.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = resolve(HERE, "../..");
const DRIVERS_TYPES_PATH = resolve(PACKAGE_ROOT, "src/drivers/types.ts");
const RUNTIME_DB_PATH = resolve(PACKAGE_ROOT, "src/runtime/db.ts");
const TYPECHECK_FIXTURES_DIR = resolve(HERE, "fixtures");

function toModuleSpecifier(fromDir: string, toFile: string): string {
  const rel = relative(fromDir, toFile).split(sep).join("/");
  return rel.startsWith(".") ? rel : `./${rel}`;
}

function createGeneratedAppSource(tempDir: string, buildSchema: () => void): string {
  resetCollectedState();

  try {
    buildSchema();

    return generateTypes(schemaToWasm(getCollectedSchema())).replace(
      'import type { WasmSchema, QueryBuilder } from "jazz-tools";',
      [
        `import type { WasmSchema } from "${toModuleSpecifier(tempDir, DRIVERS_TYPES_PATH)}";`,
        `import type { QueryBuilder } from "${toModuleSpecifier(tempDir, RUNTIME_DB_PATH)}";`,
      ].join("\n"),
    );
  } finally {
    resetCollectedState();
  }
}

function compileTypecheckFixture({
  buildSchema,
  typecheckSource,
}: {
  buildSchema: () => void;
  typecheckSource: string;
}): readonly ts.Diagnostic[] {
  const tempDir = mkdtempSync(join(tmpdir(), "jazz-tools-codegen-typecheck-"));
  const appPath = join(tempDir, "app.ts");
  const typecheckPath = join(tempDir, "typecheck.ts");

  try {
    writeFileSync(appPath, createGeneratedAppSource(tempDir, buildSchema));
    writeFileSync(typecheckPath, typecheckSource);

    const compilerOptions: ts.CompilerOptions = {
      noEmit: true,
      strict: true,
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
      moduleResolution: ts.ModuleResolutionKind.Bundler,
      allowImportingTsExtensions: true,
      skipLibCheck: true,
    };

    const program = ts.createProgram([typecheckPath], compilerOptions);
    return ts.getPreEmitDiagnostics(program);
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
}

async function compileTypecheckFixtureDir(
  fixtureName: string,
  typecheckFileName = "typecheck.ts",
): Promise<readonly ts.Diagnostic[]> {
  const fixtureDir = resolve(TYPECHECK_FIXTURES_DIR, fixtureName);
  const schemaModule = (await import(pathToFileURL(join(fixtureDir, "schema.ts")).href)) as {
    buildSchema: () => void;
  };

  return compileTypecheckFixture({
    buildSchema: schemaModule.buildSchema,
    typecheckSource: readFileSync(join(fixtureDir, typecheckFileName), "utf8"),
  });
}

function formatDiagnostics(diagnostics: readonly ts.Diagnostic[]): string {
  const host: ts.FormatDiagnosticsHost = {
    getCanonicalFileName: (fileName) => fileName,
    getCurrentDirectory: () => process.cwd(),
    getNewLine: () => "\n",
  };

  return ts.formatDiagnosticsWithColorAndContext([...diagnostics], host);
}

function diagnosticCodes(diagnostics: readonly ts.Diagnostic[]): number[] {
  return diagnostics.map((diagnostic) => diagnostic.code).sort((a, b) => a - b);
}

describe("generated codegen typechecking", () => {
  it("typechecks include collisions, widened includes, and non-overlapping relations", async () => {
    const diagnostics = await compileTypecheckFixtureDir("include-collisions");

    expect(diagnostics, formatDiagnostics(diagnostics)).toHaveLength(0);
  });

  it("typechecks nested include selectors for reverse array relations", async () => {
    const diagnostics = await compileTypecheckFixtureDir("nested-reverse-array");

    expect(diagnostics, formatDiagnostics(diagnostics)).toHaveLength(0);
  });

  it("reports compile errors for unsafe access on maybe-included overlapping relations", async () => {
    const diagnostics = await compileTypecheckFixtureDir(
      "include-collisions",
      "negative-typecheck.ts",
    );

    expect(diagnostics.length, formatDiagnostics(diagnostics)).toBeGreaterThan(0);
    expect(diagnosticCodes(diagnostics)).toContain(2339);
  });
});
