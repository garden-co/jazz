import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { join } from "node:path";
import { build } from "esbuild";
import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import ts from "typescript";

const packageDir =
  process.env.JAZZ_CLASSIC_CONSUMER_DIR ?? fileURLToPath(new URL("..", import.meta.url));

function runConsumer(source) {
  const result = spawnSync(process.execPath, ["--input-type=module", "--eval", source], {
    cwd: packageDir,
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.error?.message ?? result.stderr);
}

test("Classic co and z named imports lead to migration guidance, not a linking error", () => {
  runConsumer(`
    import assert from "node:assert/strict";
    import { co, z } from "jazz-tools";

    assert.throws(() => co.map({ title: z.string() }), error => {
      assert.equal(error.code, "JAZZ_CLASSIC_API_REMOVED");
      assert.match(error.message, /co.map/);
      assert.match(error.message, /Jazz Classic/);
      assert.match(error.message, /Jazz 2/);
      assert.ok(error.message.includes("node_modules/jazz-tools/README.md"));
      assert.ok(error.message.includes("https://jazz.tools/llms-full.txt"));
      return true;
    });
  `);
});

test("Classic namespaces and classes reject reads, calls, construction and subclassing", () => {
  runConsumer(`
    import assert from "node:assert/strict";
    import * as jazz from "jazz-tools";
    const check = (operation, name) => assert.throws(operation, error =>
      error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes(name));

    for (const name of [
      "co", "z", "CoMap", "CoList", "CoFeed", "CoPlainText", "CoRichText",
      "FileStream", "Account", "Group", "Profile",
    ]) {
      check(() => jazz[name].create, name + ".create");
    }
    check(() => jazz.z.string(), "z.string");
    check(() => jazz.Group.create(), "Group.create");
    check(() => jazz.CoMap(), "CoMap");
    check(() => new jazz.CoMap(), "CoMap");
    check(() => class Task extends jazz.CoMap {}, "CoMap");
  `);
});

for (const [entrypoint, provider] of [
  ["react", "JazzReactProvider"],
  ["react-core", null],
  ["react-native", "JazzReactNativeProvider"],
  ["expo", "JazzExpoProvider"],
]) {
  test(`Classic ${entrypoint} hooks fail directly and providers fail only when rendered`, () => {
    // Expo's public barrel needs native peers. Substitute only those host modules,
    // and fail if used; this is export/render coverage, not Expo device coverage.
    const expoHostAdapters = `
      import { register } from "node:module";
      const host = "data:text/javascript," + encodeURIComponent(\`
        const unavailable = () => { throw new Error("Unexpected Expo host call"); };
        export {
          unavailable as getRandomBytes, unavailable as getItemAsync,
          unavailable as setItemAsync, unavailable as deleteItemAsync,
        };
      \`);
      register("data:text/javascript," + encodeURIComponent(\`
        export function resolve(specifier, context, nextResolve) {
          if (specifier === "expo-crypto" || specifier === "expo-secure-store") {
            return { url: \${JSON.stringify(host)}, shortCircuit: true };
          }
          return nextResolve(specifier, context);
        }
      \`), import.meta.url);
    `;
    runConsumer(`
      import assert from "node:assert/strict";
      import { createElement, isValidElement } from "react";
      import { renderToString } from "react-dom/server";
      ${entrypoint === "expo" ? expoHostAdapters : ""}
      const binding = await import("jazz-tools/${entrypoint}");
      for (const name of ["useCoState", "useAccount", "useSuspenseCoState", "useSuspenseAccount"]) {
        assert.throws(() => binding[name](), error =>
          error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes(name));
      }
      const provider = ${JSON.stringify(provider)};
      if (provider) {
        const element = createElement(binding[provider], {}, "child");
        assert.ok(isValidElement(element));
        assert.throws(() => renderToString(element), error =>
          error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes(provider));
      }
      assert.equal(renderToString(createElement("p", {}, "Jazz 2")), "<p>Jazz 2</p>");
    `);
  });
}

test("ordinary production tree shaking preserves Classic failure side effects", async () => {
  const require = createRequire(join(packageDir, "package.json"));
  for (const operation of ["co.map;", "co.map({});", "new CoMap();"]) {
    const result = await build({
      stdin: {
        contents: `
          import assert from "node:assert/strict";
          import { co, CoMap } from ${JSON.stringify(require.resolve("jazz-tools"))};
          assert.throws(() => { ${operation} }, { code: "JAZZ_CLASSIC_API_REMOVED" });
        `,
        resolveDir: packageDir,
      },
      bundle: true,
      minify: true,
      treeShaking: true,
      packages: "external",
      platform: "node",
      format: "esm",
      write: false,
    });
    runConsumer(result.outputFiles[0].text);
  }
});

test("unselected Classic names retain ordinary missing-export errors", () => {
  const result = spawnSync(
    process.execPath,
    ["--input-type=module", "--eval", 'import { Inbox } from "jazz-tools";'],
    { cwd: packageDir, encoding: "utf8" },
  );
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /does not provide an export named 'Inbox'/);
});

test("published declarations reject Classic use and accept Jazz 2 schemas and React", () => {
  const fixtureDir = mkdtempSync(join(packageDir, ".classic-api-types-"));
  const fixture = join(fixtureDir, "consumer.tsx");
  try {
    copyFileSync(new URL("../src/classic-api.typecheck.tsx", import.meta.url), fixture);
    const program = ts.createProgram([fixture], {
      noEmit: true,
      strict: true,
      skipLibCheck: true,
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      jsx: ts.JsxEmit.ReactJSX,
      esModuleInterop: true,
      types: ["node", "react"],
    });
    const diagnostics = ts.getPreEmitDiagnostics(program);
    assert.equal(
      diagnostics.length,
      0,
      ts.formatDiagnosticsWithColorAndContext(diagnostics, {
        getCanonicalFileName: (file) => file,
        getCurrentDirectory: () => packageDir,
        getNewLine: () => "\n",
      }),
    );
  } finally {
    rmSync(fixtureDir, { recursive: true, force: true });
  }
});
