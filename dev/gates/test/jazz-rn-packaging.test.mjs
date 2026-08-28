import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import test from "node:test";
import { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { parse } from "yaml";

const require = createRequire(import.meta.url);
const ts = require("typescript");
const packageJson = JSON.parse(
  await readFile(new URL("../../../crates/jazz-rn/package.json", import.meta.url), "utf8"),
);
const withJazzRn = require("../../../crates/jazz-rn/app.plugin.js");

// This is a fixed ABI, not a denylist of sensitive-looking names. The public
// JavaScript surface may only probe the ABI and submit one opaque command.
// Trusted native scope admission deliberately has no entry in these tables.
const relayJsExports = new Set([
  "NativeRelayAbiRange",
  "NATIVE_RELAY_ABI",
  "executeNativeRelayCommand",
]);
const nativeSpecMethods = new Set(["getAbiVersion", "execute"]);
const androidRelayMethods = new Set(["getAbiVersion", "execute"]);
// This includes lifecycle/generated hooks, which are not JavaScript methods.
// Keeping them explicit catches accidental methods added beside the ABI.
const iosRelaySelectors = new Set([
  "init",
  "getAbiVersion",
  "execute",
  "invalidate",
  "getTurboModule",
]);

// Keep this declaration-aware rather than scanning broad source text: private
// trusted admission code may use these words, while comments must neither trip
// the receipt nor hide a JavaScript-visible declaration.
function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
}

// C/C++ translation phase 2 removes each immediately-adjacent backslash and
// LF or CRLF pair before tokenization. The legacy-macro receipt must apply
// that one lexical transformation first, otherwise a source-level splice can
// hide an RCT export/remap token from this deliberately raw check. Do not
// interpret comments, strings, or any other C/C++ syntax here: those must
// remain visible to the ban as well.
function spliceCPreprocessorLines(source) {
  return source.replace(/\\(?:\r\n|\n)/g, "");
}

function assertNoLegacyIosMacroToken(source) {
  assert.doesNotMatch(
    spliceCPreprocessorLines(source),
    /\bRCT_[A-Za-z0-9_]*(?:EXPORT|REMAP)[A-Za-z0-9_]*\b/,
    "iOS JazzRelay must contain no legacy RCT export/remap macro token",
  );
}

function assertExactNames(surface, actual, allowed) {
  const unexpected = [...actual].filter((name) => !allowed.has(name));
  const missing = [...allowed].filter((name) => !actual.has(name));
  assert.deepEqual(
    unexpected,
    [],
    `${surface} has unexpected JavaScript-facing name(s): ${unexpected.join(", ")}`,
  );
  assert.deepEqual(missing, [], `${surface} omits ABI name(s): ${missing.join(", ")}`);
}

function exportedStatements(surface, source) {
  const file = ts.createSourceFile(
    `${surface}.ts`,
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TSX,
  );
  assert.deepEqual(file.parseDiagnostics, [], `${surface} must be valid TypeScript`);
  return file.statements.filter(
    (statement) =>
      ts.isExportAssignment(statement) ||
      ts.isExportDeclaration(statement) ||
      ts.isNamespaceExportDeclaration(statement) ||
      (ts.canHaveModifiers(statement) &&
        ts
          .getModifiers(statement)
          ?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword)),
  );
}

function exportedDeclarationName(statement) {
  if (
    ts.isInterfaceDeclaration(statement) ||
    ts.isTypeAliasDeclaration(statement) ||
    ts.isFunctionDeclaration(statement) ||
    ts.isClassDeclaration(statement) ||
    ts.isEnumDeclaration(statement) ||
    ts.isModuleDeclaration(statement)
  ) {
    return statement.name?.text;
  }
  if (ts.isVariableStatement(statement) && statement.declarationList.declarations.length === 1) {
    const name = statement.declarationList.declarations[0].name;
    return ts.isIdentifier(name) ? name.text : undefined;
  }
  return undefined;
}

function assertNamedRelayReexport(surface, statement, names, typeOnly) {
  assert.ok(
    ts.isExportDeclaration(statement),
    `${surface} may only re-export its fixed relay names`,
  );
  assert.equal(statement.isTypeOnly, typeOnly, `${surface} has the wrong type-only export`);
  assert.equal(
    statement.moduleSpecifier?.text,
    "./relay",
    `${surface} must re-export from ./relay`,
  );
  assert.ok(
    statement.exportClause && ts.isNamedExports(statement.exportClause),
    `${surface} must not star-re-export the relay ABI`,
  );
  assert.deepEqual(
    statement.exportClause.elements.map((item) => item.name.text),
    names,
  );
  assert.ok(
    statement.exportClause.elements.every((item) => item.propertyName === undefined),
    `${surface} must not alias-re-export the relay ABI`,
  );
}

function assertExactNativeSpecMethod(member, name) {
  assert.ok(member, `NativeJazzRelay Spec omits ${name}`);
  assert.ok(
    ts.isMethodSignature(member),
    `NativeJazzRelay Spec member ${name} must be a method signature, not another TypeScript member kind`,
  );
  assert.ok(ts.isIdentifier(member.name));
  assert.equal(member.name.text, name);
  assert.equal(member.questionToken, undefined, `${name} must not be optional`);
  assert.equal(member.typeParameters, undefined, `${name} must not be generic`);

  if (name === "getAbiVersion") {
    assert.equal(member.parameters.length, 0, "getAbiVersion takes no arguments");
    assert.equal(
      member.type?.kind,
      ts.SyntaxKind.NumberKeyword,
      "getAbiVersion returns exactly number",
    );
    return;
  }

  assert.equal(member.parameters.length, 1, "execute takes exactly one argument");
  const parameter = member.parameters[0];
  assert.ok(ts.isIdentifier(parameter.name));
  assert.equal(parameter.name.text, "commandBase64");
  assert.equal(parameter.dotDotDotToken, undefined, "execute must not be variadic");
  assert.equal(parameter.questionToken, undefined, "execute argument must not be optional");
  assert.equal(
    parameter.type?.kind,
    ts.SyntaxKind.StringKeyword,
    "execute accepts exactly a string command",
  );
  assert.ok(
    member.type && ts.isTypeReferenceNode(member.type),
    "execute returns exactly Promise<string>",
  );
  assert.ok(ts.isIdentifier(member.type.typeName));
  assert.equal(member.type.typeName.text, "Promise");
  assert.equal(member.type.typeArguments?.length, 1);
  assert.equal(
    member.type.typeArguments?.[0]?.kind,
    ts.SyntaxKind.StringKeyword,
    "execute returns exactly Promise<string>",
  );
}

function braceBody(source, opening) {
  const openingIndex = source.indexOf(opening);
  assert.notEqual(openingIndex, -1, `could not find ${opening}`);
  const start = source.indexOf("{", openingIndex + opening.length);
  assert.notEqual(start, -1, `could not find ${opening} body`);
  let depth = 0;
  for (let index = start; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    else if (source[index] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start + 1, index);
    }
  }
  assert.fail(`unterminated ${opening} body`);
}

function assertExactTsRelaySurface(nativeSpec, relay, index) {
  const commentFreeSpec = stripComments(nativeSpec);
  const specExports = exportedStatements("NativeJazzRelay TypeScript spec", nativeSpec);
  const relayExports = exportedStatements("relay TypeScript module", relay);
  const indexExports = exportedStatements("relay package entry point", index);

  assert.equal(
    specExports.length,
    2,
    "NativeJazzRelay must expose exactly its Spec and registry lookup",
  );
  assert.ok(
    ts.isInterfaceDeclaration(specExports[0]),
    "NativeJazzRelay must only export Spec as an interface",
  );
  assert.equal(specExports[0].name.text, "Spec");
  assert.ok(
    ts.isExportAssignment(specExports[1]),
    "NativeJazzRelay must default-export its registry lookup",
  );
  assert.notEqual(specExports[1].isExportEquals, true);

  assert.equal(relayExports.length, 3, "relay must expose exactly its fixed ABI declarations");
  assert.ok(
    ts.isInterfaceDeclaration(relayExports[0]),
    "relay must export NativeRelayAbiRange as an interface",
  );
  assert.equal(relayExports[0].name.text, "NativeRelayAbiRange");
  assert.ok(
    ts.isVariableStatement(relayExports[1]),
    "relay must export NATIVE_RELAY_ABI as a variable",
  );
  assert.notEqual(
    relayExports[1].declarationList.flags & ts.NodeFlags.Const,
    0,
    "relay must export NATIVE_RELAY_ABI as a const",
  );
  assert.equal(relayExports[1].declarationList.declarations.length, 1);
  assert.equal(relayExports[1].declarationList.declarations[0].name.getText(), "NATIVE_RELAY_ABI");
  assert.ok(
    ts.isFunctionDeclaration(relayExports[2]),
    "relay must export executeNativeRelayCommand as a function",
  );
  assert.equal(relayExports[2].name?.text, "executeNativeRelayCommand");
  assertExactNames(
    "relay TypeScript module",
    new Set(relayExports.map(exportedDeclarationName).filter(Boolean)),
    relayJsExports,
  );

  assert.equal(
    indexExports.length,
    2,
    "relay package entry point must contain only its two fixed re-exports",
  );
  assertNamedRelayReexport(
    "relay package entry point",
    indexExports[0],
    ["NATIVE_RELAY_ABI", "executeNativeRelayCommand"],
    false,
  );
  assertNamedRelayReexport(
    "relay package entry point",
    indexExports[1],
    ["NativeRelayAbiRange"],
    true,
  );

  const spec = specExports[0];
  const membersByName = new Map();
  for (const member of spec.members) {
    assert.ok(
      ts.isMethodSignature(member) && ts.isIdentifier(member.name),
      "NativeJazzRelay TurboModule may contain only named method signatures",
    );
    assert.ok(
      nativeSpecMethods.has(member.name.text),
      `NativeJazzRelay TurboModule has unexpected member ${member.name.text}`,
    );
    assert.ok(
      !membersByName.has(member.name.text),
      `NativeJazzRelay TurboModule repeats member ${member.name.text}`,
    );
    membersByName.set(member.name.text, member);
  }
  assert.equal(
    spec.members.length,
    nativeSpecMethods.size,
    "NativeJazzRelay TurboModule has exactly its two ABI methods",
  );
  assertExactNames("NativeJazzRelay TurboModule", new Set(membersByName.keys()), nativeSpecMethods);
  for (const name of nativeSpecMethods) {
    assertExactNativeSpecMethod(membersByName.get(name), name);
  }
  assert.match(
    commentFreeSpec,
    /^\s*export\s+default\s+TurboModuleRegistry\.get<Spec>\('JazzRelay'\);\s*$/m,
    "NativeJazzRelay may only default-export its registry lookup",
  );
}

function assertOpaqueAndroidRelaySurface(androidModule) {
  const module = braceBody(
    stripComments(androidModule),
    "public class JazzRelayModule extends NativeJazzRelaySpec",
  );
  const exportedMethods = new Set();
  const declarations =
    /((?:@[A-Za-z_$][A-Za-z0-9_$.]*(?:\s*\([^{}]*\))?\s*)+)public\s+[^{};]+?\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\([^{};]*\)\s*(?:throws\s+[^{}]+)?\{/g;
  for (const match of module.matchAll(declarations)) {
    // Generated TurboModule methods use @Override; handwritten React Native
    // modules use @ReactMethod, including its fully-qualified spelling.
    if (/(?:@Override\b|@[A-Za-z_$][A-Za-z0-9_$.]*\.ReactMethod\b|@ReactMethod\b)/.test(match[1])) {
      exportedMethods.add(match[2]);
    }
  }
  assertExactNames("Android JavaScript export", exportedMethods, androidRelayMethods);
}

function objcMethodNames(source) {
  return new Set(
    [...source.matchAll(/^[+-]\s*\([^)]*\)\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*(?::|\{)/gm)].map(
      (match) => match[1],
    ),
  );
}

function objcImplementations(source) {
  const implementations = [];
  const pattern = /^\s*@implementation\s+([A-Za-z_$][A-Za-z0-9_$]*)(?:\s*\([^)]*\))?/gm;
  let match;
  while ((match = pattern.exec(source)) !== null) {
    const end = /^\s*@end\b/m.exec(source.slice(pattern.lastIndex));
    assert.ok(end, `unterminated Objective-C implementation for ${match[1]}`);
    implementations.push({
      className: match[1],
      source: source.slice(match.index, pattern.lastIndex + end.index + end[0].length),
    });
    pattern.lastIndex += end.index + end[0].length;
  }
  return implementations;
}

function assertOpaqueIosRelaySurface(iosRelay) {
  // The generated New-Architecture spec is the sole iOS JavaScript ABI. A raw
  // source ban is intentional: comments, strings, categories, and line
  // continuations must not create a parser-dependent escape hatch.
  assertNoLegacyIosMacroToken(iosRelay);
  const commentFreeRelay = stripComments(iosRelay);
  const implementations = objcImplementations(commentFreeRelay);
  const relayImplementations = implementations.filter(
    (implementation) => implementation.className === "JazzRelay",
  );
  assert.ok(relayImplementations.length > 0, "could not find JazzRelay Objective-C implementation");
  assert.ok(
    implementations.some(
      (implementation) => implementation.className === "JazzRelayTrustedAdmission",
    ),
    "could not find the specifically named trusted Objective-C admission class",
  );
  // Categories are separate @implementation JazzRelay (...) blocks. Check all
  // of them so no selector grows the generated ABI.
  const relayModule = relayImplementations.map((item) => item.source).join("\n");
  const exportedMethods = objcMethodNames(relayModule);
  assertExactNames("iOS JazzRelay implementation", exportedMethods, iosRelaySelectors);
}

test("jazz-rn publishes an Expo config plugin for a New Architecture development build", () => {
  const original = {
    name: "example",
    ios: { bundleIdentifier: "dev.jazz.example" },
  };
  const configured = withJazzRn(original);

  assert.equal(configured.newArchEnabled, true);
  assert.equal(original.newArchEnabled, undefined, "plugin must not mutate Expo's input config");
  assert.equal(configured.ios, original.ios, "unrelated Expo configuration is preserved");
  assert.equal(packageJson.exports["./app.plugin"], "./app.plugin.js");
  assert.equal(packageJson.files.includes("app.plugin.js"), true);
  assert.equal(packageJson.files.includes("scripts"), true);
  assert.equal(packageJson.peerDependencies.expo, ">=54");
  assert.equal(
    packageJson.repository.url,
    "git+https://github.com/garden-co/jazz.git",
    "the published package must point install users at its maintained source repository",
  );
  assert.equal(packageJson.bugs.url, "https://github.com/garden-co/jazz/issues");
});

test("the canonical Expo scaffold really prebuilds both relay-only platforms", () => {
  const root = new URL("../../../", import.meta.url);
  const expoBin = new URL(
    "../../../examples/todo-client-localfirst-expo/node_modules/.bin/expo",
    import.meta.url,
  );
  assert.ok(
    existsSync(expoBin),
    "Expo prebuild requires the example workspace dependencies. Run `pnpm install --frozen-lockfile` from the repository root, then rerun this gate.",
  );
  for (const script of ["verify:expo:android", "verify:expo:ios"]) {
    execFileSync("pnpm", ["--filter", "todo-client-localfirst-expo", "run", script], {
      cwd: root,
      env: { ...process.env, CI: "1" },
      stdio: "inherit",
    });
  }

  const autolink = (platform) =>
    JSON.parse(
      execFileSync(
        process.execPath,
        [
          "--no-warnings",
          "--eval",
          "require('expo/bin/autolinking')",
          "expo-modules-autolinking",
          "react-native-config",
          "--json",
          "--platform",
          platform,
        ],
        {
          cwd: new URL("../../../examples/todo-client-localfirst-expo/", import.meta.url),
          encoding: "utf8",
        },
      ),
    );
  const androidAutolink = autolink("android");
  const iosAutolink = autolink("ios");
  assert.equal(
    androidAutolink.dependencies["jazz-rn"].platforms.android.packageInstance,
    "new JazzRelayPackage()",
  );
  assert.match(iosAutolink.dependencies["jazz-rn"].platforms.ios.podspecPath, /JazzRn\.podspec$/);

  const expoRoot = new URL("../../../examples/todo-client-localfirst-expo/", import.meta.url);
  const androidProperties = readFile(new URL("android/gradle.properties", expoRoot), "utf8");
  const androidSettings = readFile(new URL("android/settings.gradle", expoRoot), "utf8");
  const iosProperties = readFile(new URL("ios/Podfile.properties.json", expoRoot), "utf8");
  const iosPodfile = readFile(new URL("ios/Podfile", expoRoot), "utf8");

  return Promise.all([androidProperties, androidSettings, iosProperties, iosPodfile]).then(
    ([androidPropertiesText, androidSettingsText, iosPropertiesText, iosPodfileText]) => {
      assert.match(androidPropertiesText, /^newArchEnabled=true$/m);
      assert.match(androidSettingsText, /expo-autolinking-settings/);
      assert.match(androidSettingsText, /autolinkLibrariesFromCommand/);
      assert.match(iosPropertiesText, /"newArchEnabled": "true"/);
      assert.match(iosPodfileText, /use_native_modules!/);
    },
  );
});

test("jazz-rn autolinks a New-Architecture relay host without legacy artifacts", async () => {
  const [podspec, androidPackage, androidBuild, iosRelay, packageRoot, rootCargo, legacyConfig] =
    await Promise.all([
      readFile(new URL("../../../crates/jazz-rn/JazzRn.podspec", import.meta.url), "utf8"),
      readFile(
        new URL(
          "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayPackage.kt",
          import.meta.url,
        ),
        "utf8",
      ),
      readFile(new URL("../../../crates/jazz-rn/android/build.gradle", import.meta.url), "utf8"),
      readFile(new URL("../../../crates/jazz-rn/ios/JazzRelay.mm", import.meta.url), "utf8"),
      readFile(new URL("../../../crates/jazz-rn/src/index.tsx", import.meta.url), "utf8"),
      readFile(new URL("../../../Cargo.toml", import.meta.url), "utf8"),
      readFile(new URL("../../../crates/jazz-rn/ubrn.config.yaml", import.meta.url), "utf8").catch(
        () => null,
      ),
    ]);

  assert.match(podspec, /JazzNativeRelay\.xcframework/);
  assert.match(podspec, /relay_header_search_path/);
  assert.match(podspec, /current_header_paths/);
  assert.match(
    podspec,
    /HEADER_SEARCH_PATHS" => "#\{current_header_paths\} \\\"\$\(PODS_ROOT\)\/boost\\\""/,
    "the legacy RN pod branch must retain the staged relay ABI header path",
  );
  assert.match(podspec, /https:\/\/github\.com\/garden-co\/jazz\.git/);
  assert.doesNotMatch(podspec, /https:\/\/https:\/\//);
  assert.doesNotMatch(podspec, /uniffi-bindgen-react-native/);
  assert.match(podspec, /requires the React Native New Architecture/);
  assert.match(androidBuild, /relayNativeArtifactsPresent/);
  assert.match(androidBuild, /externalNativeBuild/);
  assert.doesNotMatch(androidBuild, /generated\/source\/codegen\/java/);
  assert.doesNotMatch(androidBuild, /KotlinCompile/);
  assert.doesNotMatch(androidBuild, /AndroidManifestNew/);
  assert.match(androidBuild, /requires the React Native New Architecture/);
  assert.match(androidPackage, /class JazzRelayPackage/);
  assert.doesNotMatch(androidPackage, /JazzRnModule/);
  assertNoLegacyIosMacroToken(iosRelay);
  assert.match(iosRelay, /JAZZ_RELAY_ARTIFACT_AVAILABLE/);
  assert.match(iosRelay, /jazz_native_relay_host_execute/);
  assert.match(iosRelay, /<JazzNativeRelay\/jazz_native_relay\.h>/);
  assert.match(iosRelay, /E_JAZZ_RELAY_UNAVAILABLE/);
  assert.match(iosRelay, /NativeJazzRelaySpecJSI/);
  assert.doesNotMatch(packageRoot, /NativeJazzRn|uniffi/);
  assert.doesNotMatch(rootCargo, /jazz-rn\/rust/);
  assert.equal(legacyConfig, null);
});

test("jazz-rn reserves a thin binary relay TurboModule boundary for matching native builds", async () => {
  const relay = await readFile(
    new URL("../../../crates/jazz-rn/src/relay.ts", import.meta.url),
    "utf8",
  );
  const [nativeSpec, relayIndex] = await Promise.all([
    readFile(new URL("../../../crates/jazz-rn/src/NativeJazzRelay.ts", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/src/index.tsx", import.meta.url), "utf8"),
  ]);
  const codegenGate = await readFile(
    new URL("../../../crates/jazz-rn/scripts/test-codegen.sh", import.meta.url),
    "utf8",
  );
  const androidRelay = await readFile(
    new URL(
      "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayModule.java",
      import.meta.url,
    ),
    "utf8",
  );

  assert.equal(packageJson.exports["./relay"].source, "./src/relay.ts");
  assert.equal(packageJson.scripts["test:codegen"], "bash scripts/test-codegen.sh");
  assert.match(nativeSpec, /TurboModuleRegistry\.get<Spec>\('JazzRelay'\)/);
  assert.match(nativeSpec, /execute\(commandBase64: string\): Promise<string>/);
  assert.match(relay, /getAbiVersion\(\)/);
  assert.match(relay, /matching native development or release build/);
  assert.match(codegenGate, /for platform in android ios/);
  assert.match(codegenGate, /NativeJazzRelay/);
  assert.match(codegenGate, /android-gradle/);
  assert.match(codegenGate, /codegenConfig\.android\.javaPackageName/);
  assert.match(codegenGate, /generate-specs-cli\.js/);
  assert.match(codegenGate, /class NativeJazzRelaySpec/);
  assert.match(androidRelay, /JazzRelayBridge/);
  assert.match(androidRelay, /package com\.jazzrn;/);
  assert.match(androidRelay, /class JazzRelayModule extends NativeJazzRelaySpec/);
  assert.match(androidRelay, /double getAbiVersion\(\)/);
  assert.match(androidRelay, /E_JAZZ_RELAY_UNAVAILABLE/);
});

test("trusted relay admission stays outside the JavaScript command channel", async () => {
  const relay = await readFile(
    new URL("../../../crates/jazz-rn/src/relay.ts", import.meta.url),
    "utf8",
  );
  const [nativeSpec, relayIndex] = await Promise.all([
    readFile(new URL("../../../crates/jazz-rn/src/NativeJazzRelay.ts", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/src/index.tsx", import.meta.url), "utf8"),
  ]);
  const androidBridge = await readFile(
    new URL(
      "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayBridge.kt",
      import.meta.url,
    ),
    "utf8",
  );
  const iosRelay = await readFile(
    new URL("../../../crates/jazz-rn/ios/JazzRelay.mm", import.meta.url),
    "utf8",
  );
  const androidModule = await readFile(
    new URL(
      "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayModule.java",
      import.meta.url,
    ),
    "utf8",
  );
  const header = await readFile(
    new URL("../../../crates/jazz-native-relay/include/jazz_native_relay.h", import.meta.url),
    "utf8",
  );

  assertExactTsRelaySurface(nativeSpec, relay, relayIndex);
  assertOpaqueAndroidRelaySurface(androidModule);
  assertOpaqueIosRelaySurface(iosRelay);
  assert.throws(
    () =>
      assertExactTsRelaySurface(
        nativeSpec.replace("getAbiVersion(): number;", "getAuthScope(): string;"),
        relay,
        relayIndex,
      ),
    /getAuthScope|omits ABI name/,
    "an arbitrary TypeScript accessor must not enter the generated spec",
  );
  assert.throws(
    () =>
      assertExactTsRelaySurface(
        nativeSpec.replace(
          "  execute(commandBase64: string): Promise<string>;\n}",
          "  execute(commandBase64: string): Promise<string>;\n  configure: string;\n}",
        ),
        relay,
        relayIndex,
      ),
    /method signature|member kind|configure/,
    "a property-shaped TurboModule member must not enter the fixed ABI",
  );
  assert.throws(
    () =>
      assertExactTsRelaySurface(nativeSpec, `${relay}\nexport function configure() {}`, relayIndex),
    undefined,
    "an innocuous TypeScript helper must not enlarge the relay ABI",
  );
  assert.throws(
    () =>
      assertExactTsRelaySurface(
        nativeSpec,
        relay,
        `${relayIndex}\nexport { executeNativeRelayCommand as configure } from './relay';`,
      ),
    undefined,
    "an alias re-export must not smuggle a new public relay name into the package",
  );
  assert.throws(
    () => assertExactTsRelaySurface(nativeSpec, relay, `${relayIndex}\nexport * from './relay';`),
    undefined,
    "a star re-export must not automatically publish future relay helpers",
  );
  assert.throws(
    () =>
      assertExactTsRelaySurface(
        nativeSpec,
        `${relay}\nexport { default } from './NativeJazzRelay';`,
        relayIndex,
      ),
    undefined,
    "a default re-export must not publish the raw TurboModule lookup",
  );
  for (const fixture of [
    {
      name: "a relay default export",
      relay: `${relay}\nexport default requireNativeRelay;`,
    },
    {
      name: "a relay default declaration",
      relay: `${relay}\nexport default class RelayEscapeHatch {}`,
    },
    {
      name: "a relay class export",
      relay: `${relay}\nexport class RelayEscapeHatch {}`,
    },
    {
      name: "a relay enum export",
      relay: `${relay}\nexport enum RelayEscapeHatch { Open }`,
    },
    {
      name: "a relay namespace export",
      relay: `${relay}\nexport namespace RelayEscapeHatch {}`,
    },
    {
      name: "a relay export list",
      relay: `${relay}\nexport { requireNativeRelay };`,
    },
    {
      name: "a relay type-only export",
      relay: `${relay}\nexport type RelayEscapeHatch = string;`,
    },
    {
      name: "an index default export",
      index: `${relayIndex}\nexport default executeNativeRelayCommand;`,
    },
    {
      name: "an index export from an unapproved source",
      index: `${relayIndex}\nexport { executeNativeRelayCommand } from './untrusted';`,
    },
    {
      name: "an index whitespace/comment alias",
      index: `${relayIndex}\nexport /* no aliases */ { executeNativeRelayCommand /* nope */ as configure } from './relay';`,
    },
  ]) {
    assert.throws(
      () =>
        assertExactTsRelaySurface(nativeSpec, fixture.relay ?? relay, fixture.index ?? relayIndex),
      undefined,
      `${fixture.name} must not enlarge the fixed relay ABI`,
    );
  }
  assert.doesNotThrow(
    () =>
      assertExactTsRelaySurface(
        `${nativeSpec}\n// getAuthScope and configure are prose, not ABI.`,
        `${relay}\n/* export function configure() {} */`,
        `${relayIndex}\n// export * from './untrusted';`,
      ),
    "comments must not be interpreted as public relay declarations",
  );
  assert.throws(
    () =>
      assertOpaqueAndroidRelaySurface(
        androidModule.replace("public void execute", "public void configure"),
      ),
    /configure/,
    "an arbitrary generated-looking Android method must not enter the TurboModule",
  );
  assert.throws(
    () =>
      assertOpaqueAndroidRelaySurface(
        androidModule.replace(
          "  @Override\n  public void execute",
          "  @com.facebook.react.bridge.ReactMethod(isBlockingSynchronousMethod = true)\n  public void begin(String scope) {}\n\n  @Override\n  public void execute",
        ),
      ),
    /begin/,
    "a qualified handwritten @ReactMethod must receive the same fixed ABI receipt",
  );
  assert.throws(
    () =>
      assertOpaqueAndroidRelaySurface(
        androidModule.replace(
          "  @Override\n  public void execute",
          "  @ReactMethod\n  public void configure() {}\n\n  @Override\n  public void execute",
        ),
      ),
    /configure/,
    "an unqualified handwritten @ReactMethod must receive the same fixed ABI receipt",
  );
  for (const fixture of [
    {
      name: "a URL string prefix",
      source: 'NSString *url = @"https://example.invalid/RCT_EXPORT_METHOD";',
    },
    {
      name: "a comment",
      source: "// RCT_REMAP_METHOD(begin, begin:(NSString *)scope)",
    },
    {
      name: "a prefix line splice",
      source: "RCT_\\\nEXPORT_METHOD(configure:(NSString *)scope)",
    },
    {
      name: "a middle-token CRLF line splice",
      source: "RCT_EXP\\\r\nORT_METHOD(configure:(NSString *)scope)",
    },
    {
      name: "a suffix line splice",
      source: "RCT_EXPORT_\\\nMETHOD(configure:(NSString *)scope)",
    },
    {
      name: "chained LF and CRLF line splices",
      source: "RCT_\\\nEX\\\r\nPORT_METHOD(configure:(NSString *)scope)",
    },
    {
      name: "a category",
      source:
        "@implementation JazzRelay (LegacyEscapeHatch)\nRCT_EXPORT_METHOD(configure:(NSString *)scope)\n@end",
    },
  ]) {
    assert.throws(
      () => assertOpaqueIosRelaySurface(`${iosRelay}\n${fixture.source}`),
      /legacy RCT/,
      `${fixture.name} must fail the raw iOS legacy macro ban`,
    );
  }
  assert.doesNotThrow(
    () => assertOpaqueIosRelaySurface(`${iosRelay}\nRCT_UNRELATED_METHOD()`),
    "an unknown RCT macro must not be mistaken for a legacy export/remap macro",
  );
  assert.doesNotThrow(
    () =>
      assertOpaqueIosRelaySurface(
        `${iosRelay}\nRCT_EX\\ \nPORT_METHOD(configure:(NSString *)scope)`,
      ),
    "a backslash followed by spaces is not a C/C++ phase-2 line splice",
  );
  assert.throws(
    () =>
      assertOpaqueIosRelaySurface(
        iosRelay.replace("- (void)invalidate", "- (void)configure {}\n\n- (void)invalidate"),
      ),
    /configure/,
    "an arbitrary Objective-C method must not grow the sealed relay module",
  );
  assert.match(androidBridge, /object JazzRelayTrustedAdmission/);
  assert.match(androidBridge, /TrustedRelayScopeConfig/);
  assert.match(androidBridge, /nativeAdmitTrustedScopeJson/);
  assert.match(androidBridge, /nativeRevokeTrustedScope/);
  assert.match(androidBridge, /trustedCapabilities \+=/);
  assert.match(androidBridge, /trustedCapabilities -=/);
  assert.match(androidBridge, /releaseRuntime/);
  assert.match(iosRelay, /JazzRelayTrustedAdmission/);
  assert.match(iosRelay, /jazz_native_relay_host_admit_scope_json/);
  assert.match(iosRelay, /jazz_native_relay_host_revoke_scope_capability/);
  assert.match(iosRelay, /\[trustedCapabilities addObject:capability\]/);
  assert.match(iosRelay, /\[trustedCapabilities removeObject:capability\]/);
  assert.match(header, /jazz_native_relay_host_admit_scope_json/);
  assert.match(header, /jazz_native_relay_host_revoke_scope_capability/);
});

test("relay artifact staging targets every Android ABI and iOS framework slice", async () => {
  const script = await readFile(
    new URL("../../../crates/jazz-rn/scripts/build-relay-artifacts.sh", import.meta.url),
    "utf8",
  );

  assert.equal(
    packageJson.scripts["build:relay:android"],
    "bash scripts/build-relay-artifacts.sh android",
  );
  assert.equal(packageJson.scripts["build:relay:ios"], "bash scripts/build-relay-artifacts.sh ios");
  assert.match(script, /\[arm64-v8a\]=aarch64-linux-android/);
  assert.match(script, /\[armeabi-v7a\]=armv7-linux-androideabi/);
  assert.match(script, /\[x86\]=i686-linux-android/);
  assert.match(script, /\[x86_64\]=x86_64-linux-android/);
  assert.match(script, /JazzNativeRelay\.xcframework/);
  assert.match(script, /aarch64-apple-ios-sim x86_64-apple-ios/);
  assert.match(script, /simulator_stage=.*simulator/);
  assert.match(script, /\$simulator_stage\/libjazz_native_relay\.a/);
  assert.match(script, /nativeRelayAbi/);
});

test("a dry package includes every staged native relay artifact class", async () => {
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-pack-"));
  try {
    const staged = [
      "native/include/jazz_native_relay.h",
      "android/src/main/jniLibs/arm64-v8a/libjazz_native_relay.a",
      "JazzNativeRelay.xcframework/Info.plist",
    ];
    const manifest = {
      ...packageJson,
      scripts: {},
      main: undefined,
      types: undefined,
      exports: undefined,
    };
    await writeFile(join(directory, "package.json"), `${JSON.stringify(manifest)}\n`);
    for (const path of staged) {
      const destination = join(directory, path);
      await mkdir(dirname(destination), { recursive: true });
      await writeFile(destination, "staged-native-artifact\n");
    }
    const receipt = JSON.parse(
      execFileSync("npm", ["pack", "--dry-run", "--json", "--ignore-scripts"], {
        cwd: directory,
        encoding: "utf8",
      }),
    );
    const packed = new Set(receipt[0].files.map(({ path }) => path));
    for (const path of staged) {
      assert.ok(
        packed.has(path),
        `dry package omitted staged artifact ${path}; packed: ${[...packed].join(", ")}`,
      );
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("relay verification rejects a manifest-sealed XCFramework without its device slice", async () => {
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-verify-"));
  const sourceRoot = new URL("../../../", import.meta.url);
  const sourceRevision = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: sourceRoot,
    encoding: "utf8",
  }).trim();
  const nativeRelayAbi = Number(
    /pub const NATIVE_RELAY_ABI_VERSION: u16 = (\d+);/.exec(
      readFileSync(
        new URL("../../../crates/jazz-native-relay/src/lib.rs", import.meta.url),
        "utf8",
      ),
    )?.[1],
  );
  const verifier = new URL(
    "../../../crates/jazz-rn/scripts/verify-relay-artifacts.mjs",
    import.meta.url,
  );
  const packageRoot = join(directory, "package");
  const androidRoot = join(packageRoot, "android/src/main/jniLibs");
  const iosRoot = join(packageRoot, "JazzNativeRelay.xcframework");
  const androidFiles = [
    "arm64-v8a/libjazz_native_relay.a",
    "armeabi-v7a/libjazz_native_relay.a",
    "x86/libjazz_native_relay.a",
    "x86_64/libjazz_native_relay.a",
  ];
  const iosFiles = [
    "Info.plist",
    "ios-arm64/libjazz_native_relay.a",
    "ios-arm64_x86_64-simulator/libjazz_native_relay.a",
  ];
  const info = (
    includeDevice,
    simulatorLibrary = "libjazz_native_relay.a",
  ) => `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict><key>AvailableLibraries</key><array>
${
  includeDevice
    ? "<dict><key>LibraryIdentifier</key><string>ios-arm64</string><key>LibraryPath</key><string>libjazz_native_relay.a</string><key>SupportedArchitectures</key><array><string>arm64</string></array><key>SupportedPlatform</key><string>ios</string></dict>"
    : ""
}
<dict><key>LibraryIdentifier</key><string>ios-arm64_x86_64-simulator</string><key>LibraryPath</key><string>${simulatorLibrary}</string><key>SupportedArchitectures</key><array><string>arm64</string><string>x86_64</string></array><key>SupportedPlatform</key><string>ios</string><key>SupportedPlatformVariant</key><string>simulator</string></dict>
</array></dict></plist>`;
  const writeArtifactFiles = async (root, files) => {
    for (const file of files) {
      const destination = join(root, file);
      await mkdir(dirname(destination), { recursive: true });
      await writeFile(destination, file === "Info.plist" ? info(true) : `fixture:${file}\n`);
    }
  };
  const writeManifest = async (root, destination, extra = {}) => {
    const files = [];
    const visit = async (current, relative = "") => {
      for (const entry of await (
        await import("node:fs/promises")
      ).readdir(current, {
        withFileTypes: true,
      })) {
        const nextRelative = relative ? `${relative}/${entry.name}` : entry.name;
        const next = join(current, entry.name);
        if (entry.isDirectory()) await visit(next, nextRelative);
        else {
          files.push({
            path: nextRelative,
            sha256: createHash("sha256")
              .update(await readFile(next))
              .digest("hex"),
          });
        }
      }
    };
    await visit(root);
    await mkdir(dirname(destination), { recursive: true });
    await writeFile(
      destination,
      `${JSON.stringify(
        { format: 1, nativeRelayAbi, sourceRevision, ...extra, files },
        null,
        2,
      )}\n`,
    );
  };

  try {
    await writeArtifactFiles(androidRoot, androidFiles);
    await writeArtifactFiles(iosRoot, iosFiles);
    await mkdir(join(packageRoot, "native/include"), { recursive: true });
    await writeFile(
      join(packageRoot, "native/include/jazz_native_relay.h"),
      readFileSync(
        new URL("../../../crates/jazz-native-relay/include/jazz_native_relay.h", import.meta.url),
      ),
    );
    await writeManifest(androidRoot, join(packageRoot, "android/jazz-native-relay.manifest.json"), {
      toolchain: { cargoNdk: "4.1.2" },
    });
    await writeManifest(iosRoot, join(packageRoot, "ios/jazz-native-relay.manifest.json"));
    const environment = {
      ...process.env,
      JAZZ_NATIVE_RELAY_SOURCE_REVISION: sourceRevision,
      JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION: "4.1.2",
    };
    execFileSync(
      process.execPath,
      [verifier.pathname, "--package-root", packageRoot, "android", "ios"],
      {
        env: environment,
        stdio: "pipe",
      },
    );

    const simulatorDirectory = join(iosRoot, "ios-arm64_x86_64-simulator");
    await rm(join(simulatorDirectory, "libjazz_native_relay.a"));
    await writeFile(join(simulatorDirectory, "libjazz_native_relay_simulator.a"), "fixture\n");
    await writeFile(join(iosRoot, "Info.plist"), info(true, "libjazz_native_relay_simulator.a"));
    await writeManifest(iosRoot, join(packageRoot, "ios/jazz-native-relay.manifest.json"));
    assert.throws(
      () =>
        execFileSync(
          process.execPath,
          [verifier.pathname, "--package-root", packageRoot, "android", "ios"],
          { env: environment, stdio: "pipe" },
        ),
      /inconsistent static-library names/,
      "an XCFramework must use one internal static-library basename across slices",
    );

    await rm(join(simulatorDirectory, "libjazz_native_relay_simulator.a"));
    await writeFile(join(simulatorDirectory, "libjazz_native_relay.a"), "fixture\n");

    await writeFile(join(iosRoot, "Info.plist"), info(false));
    await writeManifest(iosRoot, join(packageRoot, "ios/jazz-native-relay.manifest.json"));
    assert.throws(
      () =>
        execFileSync(
          process.execPath,
          [verifier.pathname, "--package-root", packageRoot, "android", "ios"],
          { env: environment, stdio: "pipe" },
        ),
      /missing its device static-library slice/,
      "a hash-valid manifest with only the simulator library is not releasable",
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("alpha verification preserves a reusable preview's sealed source commit across a merge commit", () => {
  const selector = new URL(
    "../../../dev/artifacts/release-artifact-source-revision.mjs",
    import.meta.url,
  );
  const previewCommit = "a".repeat(40);
  const mergeCommit = "b".repeat(40);
  const select = (reuse) =>
    execFileSync(process.execPath, [selector.pathname], {
      encoding: "utf8",
      env: {
        ...process.env,
        GITHUB_SHA: mergeCommit,
        JAZZ_REUSE_PREVIEW_ARTIFACTS: reuse ? "true" : "false",
        JAZZ_RELEASE_PR_HEAD_SHA: previewCommit,
      },
    }).trim();

  // The release workflow has already established that these commits have the
  // same source tree. Their identities intentionally differ: the latter is a
  // merge commit, while the former is what the preview manifest sealed.
  assert.equal(select(true), previewCommit);
  assert.equal(select(false), mergeCommit);
  assert.throws(
    () =>
      execFileSync(process.execPath, [selector.pathname], {
        encoding: "utf8",
        stdio: "ignore",
        env: {
          ...process.env,
          GITHUB_SHA: mergeCommit,
          JAZZ_REUSE_PREVIEW_ARTIFACTS: "true",
          JAZZ_RELEASE_PR_HEAD_SHA: "not-a-commit",
        },
      }),
    (error) => error?.status === 1,
  );
});

test("release, preview, and labeled platform gates seal and link the staged relay package", async () => {
  const [packageBuild, alphaPublish, previewBuild, rnWorkflow, artifactScript, verifier] =
    await Promise.all([
      readFile(
        new URL("../../../.github/workflows/build-jazz-packages.yml", import.meta.url),
        "utf8",
      ),
      readFile(
        new URL("../../../.github/workflows/publish-jazz-tools-alpha.yml", import.meta.url),
        "utf8",
      ),
      readFile(new URL("../../../.github/workflows/preview-build.yml", import.meta.url), "utf8"),
      readFile(
        new URL("../../../.github/workflows/rn-native-artifacts.yml", import.meta.url),
        "utf8",
      ),
      readFile(
        new URL("../../../crates/jazz-rn/scripts/build-relay-artifacts.sh", import.meta.url),
        "utf8",
      ),
      readFile(
        new URL("../../../crates/jazz-rn/scripts/verify-relay-artifacts.mjs", import.meta.url),
        "utf8",
      ),
    ]);
  const packageBuildWorkflow = parse(packageBuild);
  const previewBuildWorkflow = parse(previewBuild);

  assert.match(packageBuild, /build-jazz-rn-android/);
  assert.match(packageBuild, /build-jazz-rn-ios/);
  assert.match(
    packageBuild,
    /include_rn:[\s\S]*type: boolean[\s\S]*default: false/,
    "the reusable package build must make RN release assembly an explicit opt-in",
  );
  assert.match(packageBuild, /if: inputs\.include_rn/);
  assert.match(packageBuild, /assemble-jazz-rn:[\s\S]*always\(\)[\s\S]*inputs\.include_rn/);
  assert.match(
    packageBuild,
    /jazz_rn_artifact:[\s\S]*value: \$\{\{ jobs\.assemble-jazz-rn\.outputs\.artifact \}\}/,
    "a disabled RN job must produce an empty reusable-workflow output rather than a missing reference",
  );
  assert.match(packageBuild, /name: jazz-rn-relay-android/);
  assert.match(packageBuild, /name: jazz-rn-relay-ios/);
  assert.match(packageBuild, /name: pkg-jazz-rn/);
  assert.match(packageBuild, /verify-relay-artifacts\.mjs android ios/);
  assert.match(packageBuild, /Build jazz-rn TypeScript package/);
  assert.match(alphaPublish, /pkg-jazz-rn/);
  assert.match(alphaPublish, /JAZZ_REUSE_PREVIEW_ARTIFACTS/);
  assert.match(alphaPublish, /release-artifact-source-revision\.mjs/);
  assert.match(alphaPublish, /Verify packed jazz-rn relay payload/);
  assert.match(alphaPublish, /Publish jazz-rn \(alpha tag\)/);
  assert.match(
    alphaPublish,
    /uses: \.\/\.github\/workflows\/build-jazz-packages\.yml[\s\S]*include_rn: true/,
  );
  assert.match(previewBuild, /types: \[labeled, unlabeled, synchronize, reopened\]/);
  assert.match(previewBuild, /'preview-build'/);
  assert.match(previewBuild, /'rn-preview-release'/);
  assert.match(previewBuild, /include_rn:[\s\S]*rn-preview-release/);
  assert.match(
    previewBuild,
    /github\.event\.pull_request\.head\.repo\.full_name == github\.repository/,
    "native preview packaging must not treat a maintainer-applied label as trust for fork code",
  );
  for (const guardedPreviewExpression of [
    previewBuildWorkflow.jobs.build.if,
    previewBuildWorkflow.jobs.build.with.include_rn,
    previewBuildWorkflow.jobs["publish-pkg-pr-new"].if,
  ]) {
    assert.match(
      guardedPreviewExpression,
      /github\.event\.pull_request\.head\.repo\.full_name == github\.repository/,
      "a fork label must skip the privileged build and publication paths",
    );
  }
  assert.equal(
    previewBuildWorkflow.jobs.build.secrets,
    "inherit",
    "the reusable build is privileged and must remain behind its same-repository guard",
  );
  assert.deepEqual(previewBuildWorkflow.jobs["publish-pkg-pr-new"].permissions, {
    contents: "read",
    "pull-requests": "write",
    "id-token": "write",
  });
  assert.match(previewBuild, /name: pkg-jazz-rn/);
  assert.match(previewBuild, /'\.\/crates\/jazz-rn'/);
  assert.doesNotMatch(
    previewBuild,
    /'react-native'/,
    "the React Native validation label must not cause a package preview release",
  );

  const regularPreviewPublish = previewBuild.slice(
    previewBuild.indexOf("- name: Publish to pkg.pr.new"),
    previewBuild.indexOf("- name: Publish jazz-rn to pkg.pr.new"),
  );
  assert.doesNotMatch(
    regularPreviewPublish,
    /'\.\/crates\/jazz-rn'/,
    "ordinary preview-build runs must neither download nor publish jazz-rn",
  );
  assert.throws(
    () =>
      assert.match(
        regularPreviewPublish.replace("'./packages/create-jazz'", "'./crates/jazz-rn'"),
        /ordinary preview-build runs must neither download nor publish jazz-rn/,
      ),
    /ordinary preview-build runs must neither download nor publish jazz-rn/,
  );

  const previewMode = (labels, sameRepository) => ({
    runs:
      sameRepository && (labels.includes("preview-build") || labels.includes("rn-preview-release")),
    includesRn: labels.includes("rn-preview-release") && sameRepository,
  });
  assert.deepEqual(previewMode([], true), { runs: false, includesRn: false });
  assert.deepEqual(previewMode(["preview-build"], false), {
    runs: false,
    includesRn: false,
  });
  assert.deepEqual(previewMode(["preview-build"], true), {
    runs: true,
    includesRn: false,
  });
  assert.deepEqual(previewMode(["rn-preview-release"], true), {
    runs: true,
    includesRn: true,
  });
  assert.deepEqual(previewMode(["rn-preview-release"], false), {
    runs: false,
    includesRn: false,
  });
  assert.deepEqual(previewMode(["preview-build", "rn-preview-release", "react-native"], false), {
    runs: false,
    includesRn: false,
  });
  assert.deepEqual(previewMode(["preview-build", "rn-preview-release", "react-native"], true), {
    runs: true,
    includesRn: true,
  });
  assert.deepEqual(previewBuildWorkflow.on.pull_request.types, [
    "labeled",
    "unlabeled",
    "synchronize",
    "reopened",
  ]);
  assert.equal(
    packageBuildWorkflow.on.workflow_call.inputs.include_rn.default,
    false,
    "the reusable package build defaults to its fast, non-RN path",
  );
  assert.match(previewBuildWorkflow.jobs.build.with.include_rn, /rn-preview-release/);
  assert.match(rnWorkflow, /Android relay linked AAR/);
  assert.match(rnWorkflow, /:app:assembleDebug/);
  assert.match(rnWorkflow, /iOS relay linked app/);
  assert.match(rnWorkflow, /pod install/);
  assert.match(rnWorkflow, /xcodebuild/);
  assert.match(artifactScript, /sourceRevision/);
  assert.match(verifier, /JAZZ_NATIVE_RELAY_SOURCE_REVISION/);
  assert.match(verifier, /AvailableLibraries/);
  assert.match(verifier, /requiredRoles/);
  assert.match(packageBuild, /cargo-ndk@\$\{\{ env\.JAZZ_RN_CARGO_NDK_VERSION \}\}/);
  assert.match(packageBuild, /--package-root/);
  assert.match(verifier, /relay artifact inventory differs from its manifest/);
});
