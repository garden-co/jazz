import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, matchesGlob, relative } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const srcRoot = fileURLToPath(new URL(".", import.meta.url));

/** Shipped sources: what `tsc` compiles into dist/ (see tsconfig.json's excludes). */
function shippedSources(dir = srcRoot): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = `${dir}/${entry.name}`;
    if (entry.isDirectory()) {
      return /^(fixtures|__fixtures__|testing|client-tests)$/.test(entry.name)
        ? []
        : shippedSources(path);
    }
    if (!/\.tsx?$/.test(entry.name) || entry.name.endsWith(".d.ts")) return [];
    if (/\.(test|typecheck)\.tsx?$|test-helpers|test-utils/.test(entry.name)) return [];
    return [path];
  });
}

function distPath(path: string): string {
  return `./dist/${relative(srcRoot, path).replace(/\.tsx?$/, ".js")}`;
}

/**
 * Top-level statements that run code on import, other than declarations.
 * A bare import counts unless it names a module the list keeps anyway:
 * bundlers still run a listed module that an included module imports.
 */
function topLevelEffects(path: string, listed: (distPath: string) => boolean = () => false) {
  const file = ts.createSourceFile(path, readFileSync(path, "utf8"), ts.ScriptTarget.Latest, true);
  return file.statements.flatMap((statement) => {
    if (ts.isImportDeclaration(statement)) {
      if (statement.importClause) return [];
      const specifier = (statement.moduleSpecifier as ts.StringLiteral).text;
      const target = specifier.startsWith(".")
        ? `./dist/${relative(srcRoot, join(dirname(path), specifier))}`
        : undefined;
      return target && listed(target) ? [] : [statement.getText(file)];
    }
    if (ts.isExpressionStatement(statement)) {
      // `void value;` only marks a binding as used.
      return ts.isVoidExpression(statement.expression) ? [] : [statement.getText(file)];
    }
    return ts.isIfStatement(statement) ||
      ts.isForStatement(statement) ||
      ts.isForOfStatement(statement) ||
      ts.isTryStatement(statement) ||
      ts.isBlock(statement)
      ? [statement.getText(file)]
      : [];
  });
}

describe("package.json sideEffects", () => {
  // Bundlers drop any module the list doesn't name once nothing uses its
  // exports. A module that patches globals or starts a worker on import has
  // to be listed, or apps lose that effect without an error.
  it("names every shipped module that runs code on import", () => {
    const { sideEffects } = JSON.parse(readFileSync(`${packageRoot}/package.json`, "utf8")) as {
      sideEffects: string[];
    };
    const listed = (path: string) => sideEffects.some((pattern) => matchesGlob(path, pattern));
    const unlisted = shippedSources()
      .filter((path) => topLevelEffects(path, listed).length > 0)
      .map(distPath)
      .filter((path) => !listed(path));
    expect(unlisted).toEqual([]);
  });

  it("the scan finds the modules it has to keep", () => {
    // Guards the scan itself: these run code on import today.
    for (const path of [
      "expo/polyfills.ts",
      "react-native/abort-signal.ts",
      "worker/jazz-broker-worker.ts",
    ]) {
      expect(topLevelEffects(`${srcRoot}/${path}`), path).not.toEqual([]);
    }
  });
});
