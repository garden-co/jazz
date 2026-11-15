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
      return /^(fixtures|__fixtures__|__typecheck__|testing|client-tests)$/.test(entry.name)
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

/** Built-ins whose construction only creates a value for the module itself. */
const PURE_CALLS = new Set([
  "WeakMap",
  "WeakSet",
  "Map",
  "Set",
  "RegExp",
  "URL",
  "TextEncoder",
  "TextDecoder",
  "Symbol",
  "Symbol.for",
  "Object.freeze",
  "Object.fromEntries",
  "Object.getOwnPropertyNames",
  "Array.from",
  "Uint8Array.from",
  "String",
  "Promise",
  "Promise.resolve",
  // Array and TextEncoder methods on a value the module just built.
  "<value>.map",
  "<value>.filter",
  "<value>.join",
  "<value>.encode",
]);

/**
 * Top-level calls reviewed as side-effect-free: each builds a value the
 * module keeps for itself, so dropping the unused module drops nothing else.
 * Keyed `<file>: <callee>`. A new entry needs the same review.
 */
const REVIEWED_CALLS = new Set([
  // Generated bindings bind their app, a value the module exports.
  "backend/vercel-store.ts: <value>.repeat",
  "classic-api.ts: classicFunction",
  "classic-api.ts: classicNamespace",
  "dev/app-files.ts: join",
  "dev/project.ts: join",
  "upgrade/index.ts: join",
  "upgrade/project.ts: join",
  "dev/inspector-overlay/loader.ts: HostBinding",
  "inspect/mcp.ts: WHERE_OPERATORS.join",
  "local-first-auth/identity.ts: utf8",
  "permissions/index.ts: removedSchemaApi",
  "prosemirror/overlays.ts: PluginKey",
  "prosemirror/sync.ts: PluginKey",
  "react-core/provider.tsx: createContext",
  "react-core/session.tsx: createContext",
  "removed-api.ts: removed",
  // Each builds a throwing stub (or a frozen object of them) for the module to export.
  "removed-schema-api.ts: removedNamespace",
  "removed-schema-api.ts: removedSchemaApi",
  "runtime/author-id.ts: Utf8Decoder",
  "runtime/native-runtime/native-codec.ts: Utf8Decoder",
  "runtime/native-runtime/native-row-codec.ts: Utf8Decoder",
  "runtime/native-runtime/native-runtime-adapter.ts: Utf8Decoder",
  "runtime/util.ts: Utf8Decoder",
  "runtime/json-schema-validator.ts: DRAFT4.plain.filter",
  "runtime/json-schema-validator.ts: DRAFT7.plain.filter",
  "runtime/json-schema-validator.ts: DRAFT2019.single.filter",
  "runtime/json-schema-validator.ts: DRAFT2019.plain.filter",
  "runtime/json-schema-validator.ts: base64Pattern",
  "runtime/json-schema-validator.ts: base32Pattern",
  // Adds the session object it returns to a module-local WeakSet.
  "runtime/system-identity.ts: markTrustedReservedSession",
  "subscriptions-orchestrator.ts: makeDeferred",
  "thin/self-signed-token.ts: parseUuidText",
  "vue/provider.ts: defineComponent",
  "vue/session.ts: defineComponent",
  "worker/jazz-broker-worker-core.ts: crypto.randomUUID",
]);

function isNamePath(node: ts.Expression): boolean {
  return (
    ts.isIdentifier(node) || (ts.isPropertyAccessExpression(node) && isNamePath(node.expression))
  );
}

/**
 * Calls a module makes while it initializes: in top-level variable
 * initializers, `export default`, and class static blocks and fields.
 * Function bodies don't run on import, so they're skipped.
 */
function initializerCalls(path: string): string[] {
  const file = ts.createSourceFile(path, readFileSync(path, "utf8"), ts.ScriptTarget.Latest, true);
  const calls: string[] = [];
  const visit = (node: ts.Node) => {
    if (ts.isFunctionLike(node) || ts.isClassExpression(node)) return;
    if (ts.isCallExpression(node) || ts.isNewExpression(node)) {
      // A method called on a computed value (`[…].map(…)`, `new X().y()`)
      // is named by the method alone.
      const callee = node.expression;
      calls.push(
        ts.isPropertyAccessExpression(callee) && !isNamePath(callee.expression)
          ? `<value>.${callee.name.text}`
          : callee.getText(file),
      );
    }
    ts.forEachChild(node, visit);
  };
  for (const statement of file.statements) {
    if (ts.isVariableStatement(statement)) {
      for (const declaration of statement.declarationList.declarations) {
        if (declaration.initializer) visit(declaration.initializer);
      }
    } else if (ts.isExportAssignment(statement)) {
      visit(statement.expression);
    } else if (ts.isClassDeclaration(statement)) {
      for (const member of statement.members) {
        if (ts.isClassStaticBlockDeclaration(member)) calls.push("<static block>");
        else if (
          ts.isPropertyDeclaration(member) &&
          member.initializer &&
          member.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.StaticKeyword)
        )
          visit(member.initializer);
      }
    }
  }
  return calls;
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

  // A module that isn't listed may still call something while it
  // initializes. Each such call must be one that only builds a value for the
  // module (so dropping the module loses nothing): a pure built-in, or one
  // reviewed in REVIEWED_CALLS. Anything else is either listed in
  // sideEffects or reviewed here.
  it("unlisted modules only make reviewed calls while they initialize", () => {
    const { sideEffects } = JSON.parse(readFileSync(`${packageRoot}/package.json`, "utf8")) as {
      sideEffects: string[];
    };
    const listed = (path: string) => sideEffects.some((pattern) => matchesGlob(path, pattern));
    const unreviewed = shippedSources()
      .filter((path) => !listed(distPath(path)))
      .flatMap((path) =>
        initializerCalls(path)
          .filter((callee) => !PURE_CALLS.has(callee))
          .map((callee) => `${relative(srcRoot, path)}: ${callee}`),
      )
      .filter((entry) => !REVIEWED_CALLS.has(entry));
    expect(unreviewed).toEqual([]);
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
    expect(initializerCalls(`${srcRoot}/removed-schema-api.ts`)).toContain("removedNamespace");
    expect(initializerCalls(`${srcRoot}/vue/session.ts`)).toContain("defineComponent");
  });
});
