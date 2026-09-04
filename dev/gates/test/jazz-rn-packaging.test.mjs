import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import test from "node:test";
import { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";
import { cp, mkdtemp, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { parse } from "yaml";

const require = createRequire(import.meta.url);
const ts = require("typescript");
const packageJson = JSON.parse(
  await readFile(new URL("../../../crates/jazz-rn/package.json", import.meta.url), "utf8"),
);
const withJazzRn = require("../../../crates/jazz-rn/app.plugin.js");
const androidRelayArchitectures = "armeabi-v7a,arm64-v8a,x86_64";
const npmPackMachineArgs = (...args) => [
  "pack",
  "--ignore-scripts",
  "--json",
  "--foreground-scripts=false",
  "--loglevel=silent",
  ...args,
];

function parseMachineJsonWithStructuredInfoPrelude(stdout, label) {
  // Tool commands normally write one JSON document. Expo and Bob can prefix
  // it with structured informational lines, however; accept only that known
  // prelude and keep arbitrary stdout a hard failure.
  const jsonStart = stdout.search(/^[{[]/m);
  assert.notEqual(jsonStart, -1, `${label} did not emit a JSON document`);
  const prelude = stdout.slice(0, jsonStart);
  assert.match(
    prelude,
    /^(?:(?:ℹ \[[^\]\r\n]+\][^\r\n]*)?\r?\n)*$/,
    `${label} may prefix JSON only with structured tool informational lines`,
  );
  return JSON.parse(stdout.slice(jsonStart));
}

function assertRelayAndroidArchitectures(gradleProperties, label) {
  const architectures = gradleProperties.match(/^reactNativeArchitectures=(.*)$/m)?.[1];
  assert.equal(
    architectures,
    androidRelayArchitectures,
    `${label} must request exactly the three sealed relay ABIs`,
  );
}

test("machine JSON accepts structured tool info but no arbitrary stdout", () => {
  assert.deepEqual(
    parseMachineJsonWithStructuredInfoPrelude(
      'ℹ [module] inspecting React Native configuration\n{"relay":"packed"}\n',
      "Expo",
    ),
    { relay: "packed" },
  );
  assert.throws(
    () =>
      parseMachineJsonWithStructuredInfoPrelude(
        'warning: unexpected output\n{"relay":"packed"}\n',
        "tool",
      ),
    /only with structured tool informational lines/,
    "unstructured stdout must not be mistaken for a tool JSON prelude",
  );
  assert.throws(
    () =>
      parseMachineJsonWithStructuredInfoPrelude(
        'ℹ [module] inspecting React Native configuration\n{"relay":}\n',
        "Expo",
      ),
    SyntaxError,
    "an allowed informational prelude must not make malformed JSON acceptable",
  );
  assert.throws(
    () =>
      parseMachineJsonWithStructuredInfoPrelude('{"relay":"packed"}\ntrailing output\n', "Expo"),
    SyntaxError,
    "valid JSON must not permit non-whitespace trailing stdout",
  );
});

test("npm pack JSON receipts suppress lifecycle chatter", () => {
  assert.deepEqual(npmPackMachineArgs("--pack-destination", "receipt"), [
    "pack",
    "--ignore-scripts",
    "--json",
    "--foreground-scripts=false",
    "--loglevel=silent",
    "--pack-destination",
    "receipt",
  ]);
  assert.throws(
    () => JSON.parse('ℹ [typescript] compiling package sources\n[{"filename":"jazz-rn.tgz"}]\n'),
    SyntaxError,
    "the CI-observed Bob prelude must be suppressed, not parsed as npm machine JSON",
  );
  assert.throws(
    () =>
      assert.deepEqual(
        npmPackMachineArgs().filter((arg) => arg !== "--foreground-scripts=false"),
        npmPackMachineArgs(),
      ),
    /Expected values to be strictly deep-equal/,
    "a lifecycle-enabled npm pack command must not be accepted as machine-only JSON",
  );
});

function productionDependencyNames(metadata, rootPackageName) {
  const packageById = new Map(metadata.packages.map((pkg) => [pkg.id, pkg]));
  const nodeById = new Map(metadata.resolve.nodes.map((node) => [node.id, node]));
  const root = metadata.packages.find((pkg) => pkg.name === rootPackageName);
  assert.ok(root, `cargo metadata is missing ${rootPackageName}`);
  const reachable = new Set([root.id]);
  const pending = [root.id];
  while (pending.length) {
    const id = pending.pop();
    const node = nodeById.get(id);
    assert.ok(node, `cargo metadata is missing resolved node ${id}`);
    for (const dependency of node.deps) {
      if (!dependency.dep_kinds.some((kind) => kind.kind !== "dev")) continue;
      if (!reachable.has(dependency.pkg)) {
        reachable.add(dependency.pkg);
        pending.push(dependency.pkg);
      }
    }
  }
  return new Set([...reachable].map((id) => packageById.get(id)?.name));
}

test("jazz-rn relay production builds are SQLite-only on every artifact target", () => {
  const workspace = join(import.meta.dirname, "../../..");
  const targets = [
    "aarch64-linux-android",
    "armv7-linux-androideabi",
    "x86_64-linux-android",
    "aarch64-apple-ios",
    "aarch64-apple-ios-sim",
    "x86_64-apple-ios",
  ];
  for (const target of targets) {
    const metadata = JSON.parse(
      execFileSync("cargo", ["metadata", "--format-version", "1", "--filter-platform", target], {
        cwd: workspace,
        encoding: "utf8",
        maxBuffer: 10 * 1024 * 1024,
      }),
    );
    const dependencies = productionDependencyNames(metadata, "jazz-native-relay");
    assert.ok(dependencies.has("jazz-storage-sqlite"), `${target} relay must retain SQLite`);
    for (const forbidden of ["jazz-storage-rocksdb", "jazz-benchmark-guard", "tempfile"])
      assert.ok(!dependencies.has(forbidden), `${target} relay must exclude ${forbidden}`);
  }
});

// This is a fixed ABI, not a denylist of sensitive-looking names. The public
// JavaScript surface may only probe the ABI, install/open the private JSI
// factory, and submit opaque relay/foreground byte commands. Trusted native
// scope admission deliberately has no entry in these tables.
const relayJsExports = new Set([
  "NativeRelayAbiRange",
  "NativeForegroundRuntimeFactory",
  "NativeForegroundRuntime",
  "NativeForegroundCommand",
  "NativeForegroundTransactionKind",
  "NativeForegroundResponse",
  "NativeForegroundSubscriptionEvent",
  "NATIVE_RELAY_ABI",
  "installNativeForegroundRuntime",
  "encodeNativeForegroundCommand",
  "decodeNativeForegroundResponse",
  "executeNativeRelayCommand",
]);
const nativeSpecMethods = new Set(["getAbiVersion", "execute"]);
const androidRelayMethods = new Set(["getAbiVersion", "execute"]);
// This includes lifecycle/generated hooks, which are not JavaScript methods.
// Keeping them explicit catches accidental methods added beside the ABI.
const iosRelaySelectors = new Set([
  "init",
  "getAbiVersion",
  "installJSIBindingsWithRuntime",
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

function assertExactIosModuleRegistration(source) {
  const macroSource = spliceCPreprocessorLines(source);
  const registration = macroSource.match(/\bRCT_EXPORT_MODULE\s*\(\s*\)/g) ?? [];
  assert.deepEqual(
    registration,
    ["RCT_EXPORT_MODULE()"],
    "iOS JazzRelay must register exactly once so TurboModuleRegistry can discover its generated ABI",
  );
  const legacyMacros = [
    ...macroSource.matchAll(/\b(RCT_[A-Za-z0-9_]*(?:EXPORT|REMAP)[A-Za-z0-9_]*)\b/g),
  ]
    .map((match) => match[1])
    .filter((macro) => macro !== "RCT_EXPORT_MODULE");
  assert.deepEqual(
    legacyMacros,
    [],
    `iOS JazzRelay must contain no legacy RCT export/remap macro token: ${legacyMacros.join(", ")}`,
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

  if (name === "installForegroundRuntime") {
    assert.equal(member.parameters.length, 0, "installForegroundRuntime takes no arguments");
    assert.equal(
      member.type?.kind,
      ts.SyntaxKind.VoidKeyword,
      "installForegroundRuntime returns void",
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

  assertExactNames(
    "relay TypeScript module",
    new Set(relayExports.map(exportedDeclarationName).filter(Boolean)),
    new Set([
      "NativeRelayAbiRange",
      "NativeForegroundRuntimeFactory",
      "NativeForegroundRuntime",
      "NativeForegroundCommand",
      "NativeForegroundTransactionKind",
      "NativeForegroundResponse",
      "NativeForegroundSubscriptionEvent",
      "installNativeForegroundRuntime",
      "encodeNativeForegroundCommand",
      "decodeNativeForegroundResponse",
      "executeNativeRelayCommand",
    ]),
  );
  assert.equal(relayExports.length, 12, "relay must expose exactly its fixed ABI declarations");

  assert.equal(
    indexExports.length,
    2,
    "relay package entry point must contain only its fixed re-exports",
  );
  assertNamedRelayReexport(
    "relay package entry point",
    indexExports[0],
    [
      "NATIVE_RELAY_ABI",
      "NATIVE_RELAY_ABI_V1",
      "decodeNativeForegroundResponse",
      "encodeNativeForegroundCommand",
      "executeNativeRelayCommand",
      "installNativeForegroundRuntime",
    ],
    false,
  );
  assertNamedRelayReexport(
    "relay package entry point",
    indexExports[1],
    [
      "NativeForegroundCommand",
      "NativeForegroundResponse",
      "NativeForegroundRuntime",
      "NativeForegroundRuntimeFactory",
      "NativeRelayAbiRange",
    ],
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
    "NativeJazzRelay TurboModule has exactly its fixed ABI methods",
  );
  assertExactNames("NativeJazzRelay TurboModule", new Set(membersByName.keys()), nativeSpecMethods);
  assert.doesNotMatch(
    commentFreeSpec,
    /\binstallForegroundRuntime\b/,
    "the obsolete JS-triggered foreground installer must not re-enter the TurboModule ABI",
  );
  for (const name of nativeSpecMethods) {
    assertExactNativeSpecMethod(membersByName.get(name), name);
  }
  assert.match(
    commentFreeSpec,
    /^\s*export\s+default\s+TurboModuleRegistry\.get<Spec>\(["']JazzRelay["']\);\s*$/m,
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
      // This is New Architecture installation plumbing, invoked by React
      // Native while it owns a JSI runtime; it is not a JS-visible
      // TurboModule method. Keep its exact native-only type checked here so a
      // similarly named public JavaScript method cannot hide behind it.
      if (match[2] === "getBindingsInstaller") {
        assert.match(
          match[0],
          /BindingsInstallerHolder\s+getBindingsInstaller\s*\(/,
          "Android's only extra New Architecture hook must return a bindings installer",
        );
        continue;
      }
      // These React Native lifecycle hooks retain/release the native host for
      // the lifetime of this module instance. They are not TurboModule ABI.
      if (match[2] === "initialize" || match[2] === "invalidate") {
        assert.match(
          match[0],
          /void\s+(initialize|invalidate)\s*\(\s*\)/,
          "Android lifecycle hooks must stay parameterless void hooks",
        );
        continue;
      }
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
  assertExactIosModuleRegistration(iosRelay);
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

function legacyRelayHeaderSearchPaths(podspec) {
  const stagedHeader = /^\s*relay_header_search_path\s*=\s*"([^"]+)"$/m.exec(podspec)?.[1];
  assert.equal(
    stagedHeader,
    "$(PODS_TARGET_SRCROOT)/native/include",
    "the staged relay ABI header must have one exact package-relative search path",
  );

  const initialConfig =
    /s\.pod_target_xcconfig\s*=\s*\{\s*"HEADER_SEARCH_PATHS"\s*=>\s*relay_header_search_path\s*\}/m.test(
      podspec,
    );
  assert.ok(
    initialConfig,
    "the vendored XCFramework branch must install the staged relay header before the legacy branch runs",
  );

  const legacyAssignment =
    /current_header_paths\s*=\s*s\.pod_target_xcconfig&\.fetch\("HEADER_SEARCH_PATHS", ""\) \|\| ""[\s\S]*?"HEADER_SEARCH_PATHS"\s*=>\s*"#\{current_header_paths\} \\\"\$\(PODS_ROOT\)\/boost\\\""/m.test(
      podspec,
    );
  assert.ok(
    legacyAssignment,
    "the legacy RN branch must extend its existing header paths rather than replace them",
  );

  // This is the exact value CocoaPods obtains for the staged-artifact path in
  // the legacy branch: the prior value, followed by the legacy Boost include.
  return `${stagedHeader} \"$(PODS_ROOT)/boost\"`;
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

test("the canonical Expo scaffold preserves the direct native-package contract", async () => {
  const [manifestText, appConfigText, readme] = await Promise.all([
    readFile(
      new URL("../../../examples/todo-client-localfirst-expo/package.json", import.meta.url),
      "utf8",
    ),
    readFile(
      new URL("../../../examples/todo-client-localfirst-expo/app.json", import.meta.url),
      "utf8",
    ),
    readFile(
      new URL("../../../examples/todo-client-localfirst-expo/README.md", import.meta.url),
      "utf8",
    ),
  ]);
  const manifest = JSON.parse(manifestText);
  const appConfig = JSON.parse(appConfigText).expo;

  assert.equal(manifest.dependencies["jazz-rn"], "workspace:*");
  assert.equal(manifest.scripts["verify:expo"], "pnpm verify:expo:android && pnpm verify:expo:ios");
  assert.equal(appConfig.newArchEnabled, true);
  assert.deepEqual(appConfig.plugins, ["jazz-rn"]);
  assert.match(readme, /jazz-rn@alpha/);
  assert.match(readme, /direct app dependency/);
  assert.match(readme, /does \*\*not\*\* run in Expo Go/);
  assert.match(readme, /not a runnable persistent Jazz client/);
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
    parseMachineJsonWithStructuredInfoPrelude(
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
      `canonical Expo ${platform} autolinking`,
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
      assertRelayAndroidArchitectures(androidPropertiesText, "the generated Expo host");
      assert.throws(
        () =>
          assertRelayAndroidArchitectures(
            androidPropertiesText.replace(
              `reactNativeArchitectures=${androidRelayArchitectures}`,
              "reactNativeArchitectures=armeabi-v7a,arm64-v8a,x86,x86_64",
            ),
            "a planted generated Expo host",
          ),
        /exactly the three sealed relay ABIs/,
        "the package contract must reject restoring retired x86",
      );
      assert.match(androidSettingsText, /expo-autolinking-settings/);
      assert.match(androidSettingsText, /autolinkLibrariesFromCommand/);
      assert.match(iosPropertiesText, /"newArchEnabled": "true"/);
      assert.match(iosPodfileText, /use_native_modules!/);
    },
  );
});

test("React Native installation docs advertise only the currently proven package boundary", async () => {
  const [readme, installGuide, clientSetupGuide, durabilityGuide, exampleReadme, previewWorkflow] =
    await Promise.all([
      readFile(new URL("../../../crates/jazz-rn/README.md", import.meta.url), "utf8"),
      readFile(new URL("../../../docs/content/docs/install/client.mdx", import.meta.url), "utf8"),
      readFile(
        new URL("../../../docs/content/docs/getting-started/client-setup.mdx", import.meta.url),
        "utf8",
      ),
      readFile(
        new URL("../../../docs/content/docs/reference/durability-tiers.mdx", import.meta.url),
        "utf8",
      ),
      readFile(
        new URL("../../../examples/todo-client-localfirst-expo/README.md", import.meta.url),
        "utf8",
      ),
      readFile(new URL("../../../.github/workflows/preview-build.yml", import.meta.url), "utf8"),
    ]);

  assert.match(readme, /pnpm add jazz-rn@alpha/);
  assert.match(readme, /"plugins": \["jazz-rn"\]/);
  assert.match(readme, /npx expo prebuild --clean/);
  assert.match(readme, /newArchEnabled=true/);
  assert.match(readme, /RCT_NEW_ARCH_ENABLED=1 bundle exec pod install/);
  assert.match(readme, /Expo Go is not\s+supported/);
  assert.match(readme, /not yet a supported high-level React Native Jazz client/);
  assert.match(readme, /rn-preview-release/);
  assert.match(
    previewWorkflow,
    /contains\(github\.event\.pull_request\.labels\.\*\.name, 'rn-preview-release'\)/,
    "the documented preview label must still opt into the actual artifact workflow",
  );
  assert.match(
    installGuide,
    /React Native and Expo are intentionally not part of this application quickstart yet[\s\S]*not a supported React Native Jazz client/,
    "the public install guide must put the unsupported RN boundary before its runtime quickstart",
  );
  for (const [name, guide] of [
    ["install guide", installGuide],
    ["client setup guide", clientSetupGuide],
    ["durability guide", durabilityGuide],
  ]) {
    assert.doesNotMatch(
      guide,
      /<Tab value="Expo">|jazz-tools\/expo|todo-client-localfirst-expo/,
      `${name} must not retain runnable-looking Expo tabs or RN runtime snippets`,
    );
  }
  assert.match(exampleReadme, /native-relay install\/ABI boundary/);
  assert.match(exampleReadme, /not a runnable persistent Jazz client/);
});

test("a freshly installed Expo app prebuilds the packed jazz-rn relay host", async () => {
  // The canonical example is useful, but its workspace link can accidentally
  // hide npm-packaging mistakes. This receipt instead constructs the smallest
  // possible Expo app in a new directory, mounts the tarball that an adopter
  // would receive as its direct dependency, and runs the two prebuild paths.
  // An isolated npm install consumes the packed tarball in offline mode before
  // the Expo scaffold receives a copy of that installed package. The package
  // must therefore declare every direct dependency it needs; it cannot
  // accidentally reach this workspace's jazz-rn sources. The scaffold itself
  // supplies only its explicit Expo/React/React-Native peer graph.
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-fresh-expo-"));
  const packageDirectory = join(directory, "package");
  const installDirectory = join(directory, "installed-package");
  const installedNodeModules = join(installDirectory, "node_modules");
  const appDirectory = join(directory, "app");
  const appNodeModules = join(appDirectory, "node_modules");
  const bareAppDirectory = join(directory, "bare-react-native-app");
  const bareAppNodeModules = join(bareAppDirectory, "node_modules");
  const canonicalNodeModules = new URL(
    "../../../examples/todo-client-localfirst-expo/node_modules/",
    import.meta.url,
  ).pathname;
  try {
    await mkdir(packageDirectory, { recursive: true });
    const packed = JSON.parse(
      execFileSync("npm", npmPackMachineArgs("--pack-destination", packageDirectory), {
        cwd: new URL("../../../crates/jazz-rn/", import.meta.url),
        encoding: "utf8",
      }),
    );
    assert.deepEqual(packed.length, 1, "packing jazz-rn must produce one npm tarball");
    const tarball = join(packageDirectory, packed[0].filename);

    await mkdir(installDirectory, { recursive: true });
    await writeFile(
      join(installDirectory, "package.json"),
      `${JSON.stringify(
        {
          name: "jazz-rn-packed-install-receipt",
          version: "0.0.0",
          private: true,
          dependencies: { "jazz-rn": `file:../package/${packed[0].filename}` },
        },
        null,
        2,
      )}\n`,
    );
    execFileSync(
      "npm",
      ["install", "--offline", "--ignore-scripts", "--omit=peer", "--legacy-peer-deps"],
      {
        cwd: installDirectory,
        stdio: "inherit",
      },
    );
    const packedManifest = JSON.parse(
      await readFile(join(installedNodeModules, "jazz-rn", "package.json"), "utf8"),
    );
    assert.deepEqual(
      Object.keys(packedManifest.peerDependencies).sort(),
      ["expo", "react", "react-native"],
      "the packed relay must declare every host runtime it imports as a peer dependency",
    );

    await mkdir(appDirectory, { recursive: true });
    await writeFile(
      join(appDirectory, "package.json"),
      `${JSON.stringify(
        {
          name: "jazz-rn-fresh-expo-receipt",
          version: "0.0.0",
          private: true,
          dependencies: {
            expo: "54.0.37",
            "jazz-rn": `file:../package/${packed[0].filename}`,
            react: "19.2.4",
            "react-native": "0.81.5",
          },
        },
        null,
        2,
      )}\n`,
    );
    await writeFile(
      join(appDirectory, "app.json"),
      `${JSON.stringify(
        {
          expo: {
            name: "Jazz RN fresh install receipt",
            slug: "jazz-rn-fresh-install-receipt",
            version: "1.0.0",
            plugins: ["jazz-rn"],
          },
        },
        null,
        2,
      )}\n`,
    );
    await writeFile(
      join(appDirectory, "App.tsx"),
      [
        'import { NATIVE_RELAY_ABI } from "jazz-rn";',
        "",
        "export const relayAbi: number = NATIVE_RELAY_ABI.maximum;",
        "",
      ].join("\n"),
    );
    await writeFile(
      join(appDirectory, "tsconfig.json"),
      `${JSON.stringify({ extends: "expo/tsconfig.base", include: ["App.tsx"] }, null, 2)}\n`,
    );
    await mkdir(appNodeModules, { recursive: true });
    await cp(join(installedNodeModules, "jazz-rn"), join(appNodeModules, "jazz-rn"), {
      recursive: true,
      dereference: true,
    });
    for (const dependency of ["@types", "expo", "react", "react-native", "typescript"]) {
      await symlink(join(canonicalNodeModules, dependency), join(appNodeModules, dependency));
    }
    // Typechecking and autolinking prove that the packed package is present,
    // but neither executes the JavaScript an app actually imports. Run the
    // compiled entry point from the installed tarball under a deliberately
    // tiny native-module shim. The shim is a Node loader rather than a copied
    // package file: `jazz-rn` still resolves its own published
    // `NativeJazzRelay.js`, which in turn resolves the host's `react-native`
    // import in exactly the ordinary package graph.
    //
    // This is not a substitute for the linked Android/iOS receipts. It makes
    // the installation boundary executable: a tarball with stale/missing
    // compiled JavaScript, a broken package export, or a changed ABI failure
    // path cannot pass merely because its declarations and Podspec parse.
    const nativeModuleShim = join(directory, "native-relay-shim.mjs");
    const nativeModuleLoader = join(directory, "native-relay-loader.mjs");
    await writeFile(
      nativeModuleShim,
      [
        "const available = process.env.JAZZ_RN_PACKED_NATIVE_AVAILABLE === '1';",
        "const abi = Number(process.env.JAZZ_RN_PACKED_NATIVE_ABI);",
        "export const TurboModuleRegistry = {",
        "  get() {",
        "    return available ? {",
        "      getAbiVersion: () => abi,",
        "      execute: async (command) => `native:${command}`,",
        "    } : null;",
        "  },",
        "};",
        "",
      ].join("\n"),
    );
    await writeFile(
      nativeModuleLoader,
      [
        "export async function resolve(specifier, context, nextResolve) {",
        '  if (specifier !== "react-native") return nextResolve(specifier, context);',
        "  return { url: new URL('./native-relay-shim.mjs', import.meta.url).href, shortCircuit: true };",
        "}",
        "",
      ].join("\n"),
    );
    const runPackedRelay = (environment, program) =>
      execFileSync(
        process.execPath,
        [
          "--no-warnings",
          "--experimental-loader",
          nativeModuleLoader,
          "--input-type=module",
          "--eval",
          program,
        ],
        {
          cwd: appDirectory,
          env: { ...process.env, ...environment },
          encoding: "utf8",
        },
      );
    // Ask the packed JavaScript entry point for its own ABI range. The native
    // shim must track this published contract rather than pinning a historical
    // version number: the receipt is specifically meant to catch a native/JS
    // ABI mismatch after either side changes.
    const packedRelayAbi = Number(
      runPackedRelay(
        {
          JAZZ_RN_PACKED_NATIVE_AVAILABLE: "0",
          JAZZ_RN_PACKED_NATIVE_ABI: "0",
        },
        'const { NATIVE_RELAY_ABI } = await import("jazz-rn"); process.stdout.write(String(NATIVE_RELAY_ABI.maximum));',
      ),
    );
    assert.ok(
      Number.isSafeInteger(packedRelayAbi) && packedRelayAbi > 0,
      "the packed relay must export a positive ABI version for its native fixture",
    );
    assert.equal(
      runPackedRelay(
        {
          JAZZ_RN_PACKED_NATIVE_AVAILABLE: "1",
          JAZZ_RN_PACKED_NATIVE_ABI: String(packedRelayAbi),
        },
        'const { executeNativeRelayCommand } = await import("jazz-rn"); process.stdout.write(await executeNativeRelayCommand("AQI="));',
      ),
      "native:AQI=",
      "the fresh app must execute the published relay entry point through its installed native module",
    );
    for (const [name, environment, expected] of [
      [
        "missing native module",
        {
          JAZZ_RN_PACKED_NATIVE_AVAILABLE: "0",
          JAZZ_RN_PACKED_NATIVE_ABI: String(packedRelayAbi),
        },
        /native relay is unavailable.*Expo Go never includes it/i,
      ],
      [
        "incompatible native ABI",
        {
          JAZZ_RN_PACKED_NATIVE_AVAILABLE: "1",
          JAZZ_RN_PACKED_NATIVE_ABI: String(packedRelayAbi + 1),
        },
        new RegExp(
          `ABI ${packedRelayAbi + 1} is incompatible with JavaScript ABI ${packedRelayAbi}\\.\\.=${packedRelayAbi}`,
          "i",
        ),
      ],
    ]) {
      const diagnostic = runPackedRelay(
        environment,
        [
          'const { executeNativeRelayCommand } = await import("jazz-rn");',
          "try {",
          '  await executeNativeRelayCommand("AQI=");',
          '  throw new Error("packed relay unexpectedly accepted this unavailable native state");',
          "} catch (error) {",
          "  process.stdout.write(error instanceof Error ? error.message : String(error));",
          "}",
        ].join("\n"),
      );
      assert.match(
        diagnostic,
        expected,
        `the fresh packed runtime must explain ${name} before attempting relay I/O`,
      );
    }
    execFileSync(join(appNodeModules, "typescript", "bin", "tsc"), ["--noEmit"], {
      cwd: appDirectory,
      stdio: "inherit",
    });
    const bareReactNativeConfig = parseMachineJsonWithStructuredInfoPrelude(
      execFileSync(join(canonicalNodeModules, ".bin", "react-native"), ["config"], {
        cwd: appDirectory,
        encoding: "utf8",
      }),
      "bare React Native config",
    );
    assert.equal(
      bareReactNativeConfig.dependencies["jazz-rn"].platforms.android.packageInstance,
      "new JazzRelayPackage()",
      "a bare React Native host must discover the packed relay package too",
    );
    assert.match(
      bareReactNativeConfig.dependencies["jazz-rn"].platforms.ios.podspecPath,
      /JazzRn\.podspec$/,
      "a bare React Native host must discover the packed iOS podspec too",
    );
    // Expo prebuild hosts an ordinary React Native app, but it also brings
    // Expo's config/plugin machinery. Keep an explicitly bare scaffold in
    // this receipt so an Expo-specific resolver, transitive workspace link,
    // or config convention cannot accidentally stand in for the direct React
    // Native installation path.
    await mkdir(bareAppDirectory, { recursive: true });
    await writeFile(
      join(bareAppDirectory, "package.json"),
      `${JSON.stringify(
        {
          name: "jazz-rn-fresh-bare-receipt",
          version: "0.0.0",
          private: true,
          dependencies: {
            "jazz-rn": `file:../package/${packed[0].filename}`,
            react: "19.2.4",
            "react-native": "0.81.5",
          },
          devDependencies: {
            "@types/react": "19.2.14",
            typescript: "6.0.2",
          },
        },
        null,
        2,
      )}\n`,
    );
    await writeFile(
      join(bareAppDirectory, "index.js"),
      [
        'import { AppRegistry } from "react-native";',
        'import App from "./App";',
        'AppRegistry.registerComponent("JazzRnFreshBareReceipt", () => App);',
        "",
      ].join("\n"),
    );
    await writeFile(
      join(bareAppDirectory, "App.tsx"),
      [
        'import { Text } from "react-native";',
        'import { NATIVE_RELAY_ABI } from "jazz-rn";',
        "",
        "export default function App() {",
        "  return <Text>Jazz Relay ABI {NATIVE_RELAY_ABI.maximum}</Text>;",
        "}",
        "",
      ].join("\n"),
    );
    await writeFile(
      join(bareAppDirectory, "tsconfig.json"),
      `${JSON.stringify(
        {
          compilerOptions: {
            jsx: "react-jsx",
            module: "esnext",
            moduleResolution: "bundler",
            noEmit: true,
            skipLibCheck: true,
            target: "es2022",
          },
          include: ["App.tsx"],
        },
        null,
        2,
      )}\n`,
    );
    await mkdir(bareAppNodeModules, { recursive: true });
    await cp(join(installedNodeModules, "jazz-rn"), join(bareAppNodeModules, "jazz-rn"), {
      recursive: true,
      dereference: true,
    });
    for (const dependency of ["react", "react-native", "typescript"]) {
      await symlink(join(canonicalNodeModules, dependency), join(bareAppNodeModules, dependency));
    }
    await mkdir(join(bareAppNodeModules, "@types"), { recursive: true });
    await symlink(
      new URL("../../../node_modules/.pnpm/node_modules/@types/react", import.meta.url).pathname,
      join(bareAppNodeModules, "@types", "react"),
    );
    execFileSync(join(bareAppNodeModules, "typescript", "bin", "tsc"), ["--noEmit"], {
      cwd: bareAppDirectory,
      stdio: "inherit",
    });
    const directBareReactNativeConfig = parseMachineJsonWithStructuredInfoPrelude(
      execFileSync(join(canonicalNodeModules, ".bin", "react-native"), ["config"], {
        cwd: bareAppDirectory,
        encoding: "utf8",
      }),
      "direct bare React Native config",
    );
    const directBareJazzRn = directBareReactNativeConfig.dependencies["jazz-rn"];
    assert.ok(directBareJazzRn, "a direct bare host must discover the packed jazz-rn package");
    assert.throws(
      () => require.resolve("expo/package.json", { paths: [bareAppDirectory] }),
      { code: "MODULE_NOT_FOUND" },
      "the direct bare receipt must not resolve Expo from its application graph",
    );
    assert.equal(
      directBareJazzRn.root,
      join(bareAppNodeModules, "jazz-rn"),
      "a direct bare host must discover its tarball installation, not the workspace package",
    );
    assert.equal(
      directBareJazzRn.platforms.android.packageInstance,
      "new JazzRelayPackage()",
      "a direct bare host must expose the generated Android relay package to autolinking",
    );
    assert.match(
      directBareJazzRn.platforms.android.sourceDir,
      new RegExp(
        `${bareAppNodeModules.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}[\\/]jazz-rn[\\/]android$`,
      ),
      "a direct bare host must retain the Android source from its packed installation",
    );
    assert.equal(
      directBareJazzRn.platforms.ios.podspecPath,
      join(bareAppNodeModules, "jazz-rn", "JazzRn.podspec"),
      "a direct bare host must retain the iOS podspec from its packed installation",
    );
    assert.equal(
      require.resolve("jazz-rn/package.json", { paths: [bareAppDirectory] }),
      join(bareAppNodeModules, "jazz-rn", "package.json"),
      "the direct bare receipt must resolve jazz-rn from the copied packed tarball",
    );
    assert.equal(
      require.resolve("jazz-rn", { paths: [bareAppDirectory] }),
      join(bareAppNodeModules, "jazz-rn", "lib", "module", "index.js"),
      "the direct bare receipt must resolve the published JavaScript entry, not workspace source",
    );
    const packedAndroidAdmission = await readFile(
      join(
        bareAppNodeModules,
        "jazz-rn",
        "android",
        "src",
        "main",
        "java",
        "com",
        "jazzrn",
        "JazzRelayBridge.kt",
      ),
      "utf8",
    );
    assert.match(
      packedAndroidAdmission,
      /^internal object JazzRelayBridge \{/m,
      "the packed runtime bridge remains Kotlin-internal; Java compatibility is checked below",
    );
    assert.match(
      packedAndroidAdmission,
      /^object JazzRelayTrustedAdmission \{[\s\S]*^  fun beginPrivateSession\([\s\S]*\): ByteArray = JazzRelayBridge\.beginPrivateSession/m,
      "a packed consumer must be able to reference the public private-session facade",
    );
    assert.match(
      packedAndroidAdmission,
      /^  fun attachCanonicalSchema\(session: ByteArray, schemaJson: String\): ByteArray =[\s\S]*JazzRelayBridge\.attachCanonicalSchema/m,
      "a packed consumer must be able to reference the public schema facade",
    );
    assert.match(
      packedAndroidAdmission,
      /^  fun revoke\(capability: ByteArray\): Unit = JazzRelayBridge\.revokeTrustedScope/m,
      "a packed consumer must be able to revoke through the public facade",
    );
    for (const method of [
      "admitTrustedScope",
      "revokeTrustedScope",
      "beginPrivateSession",
      "attachCanonicalSchema",
    ])
      assert.match(
        packedAndroidAdmission,
        new RegExp(`@JvmSynthetic\\s+@Synchronized\\s+fun ${method}`),
        `${method} must be synthetic to Java callers`,
      );
    assert.doesNotMatch(
      packedAndroidAdmission,
      /object JazzRelayTrustedAdmission \{[\s\S]*@JvmSynthetic[\s\S]*fun beginPrivateSession/m,
      "the public admission facade must remain callable from Java as well as Kotlin",
    );
    assert.throws(
      () =>
        assert.doesNotMatch(
          packedAndroidAdmission.replace(
            "  fun beginPrivateSession(\n",
            "  @JvmSynthetic\n  fun beginPrivateSession(\n",
          ),
          /object JazzRelayTrustedAdmission \{[\s\S]*@JvmSynthetic[\s\S]*fun beginPrivateSession/m,
        ),
      /expected to not match/,
      "making the public facade Java-synthetic must fail",
    );
    assert.throws(
      () =>
        assert.match(
          packedAndroidAdmission.replace(
            "  @JvmSynthetic\n  @Synchronized\n  fun beginPrivateSession",
            "  @Synchronized\n  fun beginPrivateSession",
          ),
          /@JvmSynthetic\s+@Synchronized\s+fun beginPrivateSession/,
        ),
      /did not match/,
      "removing Java synthesis protection from a sensitive bridge method must fail",
    );

    // Kotlin `internal` compiles to public JVM bytecode. Compile the Kotlin
    // sources from the packed tarball in a standalone module, then use javac
    // as an external consumer: sensitive bridge entry points must not resolve
    // from Java, while external Kotlin and Java consumers can use the public
    // facade.
    const requireJvmAdmissionContract = process.env.JAZZ_REQUIRE_RN_JVM_CONTRACT === "1";
    const hasJvmToolchain = ["java", "javac"].every((command) => {
      try {
        execFileSync(command, ["-version"], { stdio: "ignore" });
        return true;
      } catch {
        return false;
      }
    });
    if (requireJvmAdmissionContract && !hasJvmToolchain)
      throw new Error("JAZZ_REQUIRE_RN_JVM_CONTRACT=1 requires both java and javac");
    if (hasJvmToolchain) {
      const jvmReceiptDirectory = join(directory, "packed-jvm-admission-receipt");
      const relaySourceDirectory = join(jvmReceiptDirectory, "relay", "src", "main");
      const relayKotlinDirectory = join(relaySourceDirectory, "kotlin", "com", "jazzrn");
      const relayJavaDirectory = join(relaySourceDirectory, "java");
      const consumerKotlinDirectory = join(
        jvmReceiptDirectory,
        "consumer",
        "src",
        "main",
        "kotlin",
        "consumer",
      );
      await mkdir(relayKotlinDirectory, { recursive: true });
      await mkdir(relayJavaDirectory, { recursive: true });
      await mkdir(consumerKotlinDirectory, { recursive: true });
      await writeFile(join(relayKotlinDirectory, "JazzRelayBridge.kt"), packedAndroidAdmission);
      for (const [relativePath, source] of [
        [
          "android/content/Context.java",
          "package android.content; public class Context { public java.io.File getNoBackupFilesDir() { return null; } }",
        ],
        [
          "android/util/Base64.java",
          "package android.util; public final class Base64 { public static final int NO_WRAP = 2; public static byte[] decode(String value, int flags) { return null; } public static String encodeToString(byte[] value, int flags) { return null; } }",
        ],
        [
          "com/facebook/react/turbomodule/core/interfaces/BindingsInstallerHolder.java",
          "package com.facebook.react.turbomodule.core.interfaces; public interface BindingsInstallerHolder {}",
        ],
        [
          "org/json/JSONObject.java",
          "package org.json; public class JSONObject { public static final Object NULL = new Object(); public JSONObject() {} public JSONObject(String value) {} public JSONObject put(String key, Object value) { return this; } }",
        ],
      ]) {
        const destination = join(relayJavaDirectory, relativePath);
        await mkdir(dirname(destination), { recursive: true });
        await writeFile(destination, source);
      }
      await writeFile(
        join(consumerKotlinDirectory, "FacadeConsumer.kt"),
        [
          "package consumer",
          "",
          "import android.content.Context",
          "import com.jazzrn.JazzRelayTrustedAdmission",
          "",
          "fun admit(context: Context): ByteArray = JazzRelayTrustedAdmission.beginPrivateSession(",
          '  context, "https://relay.invalid", "app", "jwt",',
          ")",
        ].join("\n"),
      );
      await writeFile(
        join(jvmReceiptDirectory, "settings.gradle"),
        'rootProject.name = "packed-jvm-admission-receipt"\ninclude(":relay", ":consumer")\n',
      );
      await writeFile(
        join(jvmReceiptDirectory, "relay", "build.gradle"),
        'plugins { id "org.jetbrains.kotlin.jvm" version "2.1.20" }\nrepositories { mavenCentral() }\n',
      );
      await writeFile(
        join(jvmReceiptDirectory, "consumer", "build.gradle"),
        'plugins { id "org.jetbrains.kotlin.jvm" version "2.1.20" }\nrepositories { mavenCentral() }\ndependencies { implementation project(":relay") }\n',
      );
      const gradle = new URL("../../../dev/rn-device-acceptance/android/gradlew", import.meta.url)
        .pathname;
      execFileSync(
        gradle,
        [
          "--offline",
          "--no-daemon",
          "-p",
          jvmReceiptDirectory,
          ":relay:classes",
          ":consumer:compileKotlin",
        ],
        { stdio: "inherit" },
      );
      const javaEscape = join(jvmReceiptDirectory, "JavaBridgeEscape.java");
      const javaFacade = join(jvmReceiptDirectory, "JavaFacadeConsumer.java");
      await writeFile(
        javaEscape,
        [
          "package consumer;",
          "import android.content.Context;",
          "import com.jazzrn.JazzRelayBridge;",
          "class JavaBridgeEscape {",
          "  byte[] admit(Context context) {",
          '    return JazzRelayBridge.INSTANCE.beginPrivateSession(context, "https://relay.invalid", "app", "jwt");',
          "  }",
          "}",
        ].join("\n"),
      );
      await writeFile(
        javaFacade,
        [
          "package consumer;",
          "import android.content.Context;",
          "import com.jazzrn.JazzRelayTrustedAdmission;",
          "class JavaFacadeConsumer {",
          "  byte[] admit(Context context) {",
          '    return JazzRelayTrustedAdmission.INSTANCE.beginPrivateSession(context, "https://relay.invalid", "app", "jwt");',
          "  }",
          "}",
        ].join("\n"),
      );
      const relayClasses = [
          join(jvmReceiptDirectory, "relay", "build", "classes", "kotlin", "main"),
          join(jvmReceiptDirectory, "relay", "build", "classes", "java", "main"),
        ].join(":"),
        javaEscapeOutput = join(jvmReceiptDirectory, "java-escape-output"),
        javaFacadeOutput = join(jvmReceiptDirectory, "java-facade-output");
      execFileSync("javac", ["-cp", relayClasses, "-d", javaFacadeOutput, javaFacade]);
      assert.throws(
        () => execFileSync("javac", ["-cp", relayClasses, "-d", javaEscapeOutput, javaEscape]),
        (error) =>
          error.status !== 0 &&
          /beginPrivateSession/.test(String(error.stderr)) &&
          /cannot find symbol/.test(String(error.stderr)),
        "a Java consumer must not resolve the packed bridge's sensitive admission methods",
      );
    }
    for (const platform of ["android", "ios"]) {
      execFileSync(
        join(canonicalNodeModules, ".bin", "expo"),
        ["prebuild", "--platform", platform, "--clean", "--no-install"],
        {
          cwd: appDirectory,
          env: { ...process.env, CI: "1" },
          stdio: "inherit",
        },
      );
    }

    const [androidProperties, androidSettings, iosProperties, iosPodfile] = await Promise.all([
      readFile(join(appDirectory, "android/gradle.properties"), "utf8"),
      readFile(join(appDirectory, "android/settings.gradle"), "utf8"),
      readFile(join(appDirectory, "ios/Podfile.properties.json"), "utf8"),
      readFile(join(appDirectory, "ios/Podfile"), "utf8"),
    ]);
    assert.match(androidProperties, /^newArchEnabled=true$/m);
    assertRelayAndroidArchitectures(androidProperties, "a packed jazz-rn install");
    assert.match(androidSettings, /autolinkLibrariesFromCommand/);
    assert.match(iosProperties, /"newArchEnabled": "true"/);
    assert.match(iosPodfile, /use_native_modules!/);

    const expoAutolink = (platform) =>
      parseMachineJsonWithStructuredInfoPrelude(
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
          { cwd: appDirectory, encoding: "utf8" },
        ),
        `packed Expo ${platform} autolinking`,
      );
    const androidAutolink = expoAutolink("android");
    const iosAutolink = expoAutolink("ios");
    assert.equal(
      androidAutolink.dependencies["jazz-rn"].platforms.android.packageInstance,
      "new JazzRelayPackage()",
      "the packed tarball, not a workspace symlink, must autolink the relay host",
    );
    assert.match(
      iosAutolink.dependencies["jazz-rn"].platforms.ios.podspecPath,
      /JazzRn\.podspec$/,
      "the packed tarball must autolink the relay iOS podspec too",
    );
    assert.equal(
      require.resolve("jazz-rn/package.json", { paths: [appDirectory] }),
      join(appNodeModules, "jazz-rn", "package.json"),
      "the receipt must resolve jazz-rn from the npm-installed packed tarball",
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("jazz-rn autolinks a New-Architecture relay host without legacy artifacts", async () => {
  const [
    podspec,
    androidPackage,
    androidBuild,
    androidCmake,
    iosRelay,
    packageRoot,
    rootCargo,
    legacyConfig,
  ] = await Promise.all([
    readFile(new URL("../../../crates/jazz-rn/JazzRn.podspec", import.meta.url), "utf8"),
    readFile(
      new URL(
        "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayPackage.kt",
        import.meta.url,
      ),
      "utf8",
    ),
    readFile(new URL("../../../crates/jazz-rn/android/build.gradle", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/android/CMakeLists.txt", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/ios/JazzRelay.mm", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/src/index.tsx", import.meta.url), "utf8"),
    readFile(new URL("../../../Cargo.toml", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/ubrn.config.yaml", import.meta.url), "utf8").catch(
      () => null,
    ),
  ]);

  assert.match(podspec, /JazzNativeRelay\.xcframework/);
  assert.equal(
    legacyRelayHeaderSearchPaths(podspec),
    '$(PODS_TARGET_SRCROOT)/native/include "$(PODS_ROOT)/boost"',
    "the legacy RN pod branch must retain the exact staged relay ABI header path",
  );
  assert.throws(
    () =>
      legacyRelayHeaderSearchPaths(
        podspec.replace(
          "$(PODS_TARGET_SRCROOT)/native/include",
          "$(PODS_TARGET_SRCROOT)/native/not-the-relay-header",
        ),
      ),
    /exact package-relative search path/,
    "the receipt must fail if the legacy branch retains a different staged-header path",
  );
  assert.match(podspec, /https:\/\/github\.com\/garden-co\/jazz\.git/);
  assert.doesNotMatch(podspec, /https:\/\/https:\/\//);
  assert.doesNotMatch(podspec, /uniffi-bindgen-react-native/);
  assert.match(podspec, /requires the React Native New Architecture/);
  assert.match(androidBuild, /relayNativeArtifactsPresent/);
  assert.match(androidBuild, /externalNativeBuild/);
  assert.match(androidBuild, /prefab true/);
  assert.match(androidBuild, /require\.resolve\('react-native\/package\.json'\)/);
  assert.match(androidBuild, /-DREACT_NATIVE_DIR=\$\{reactNativeDir\}/);
  assert.match(androidCmake, /find_package\(ReactAndroid REQUIRED CONFIG\)/);
  assert.match(androidCmake, /find_package\(fbjni REQUIRED CONFIG\)/);
  assert.match(androidCmake, /target_compile_features\(jazzrelay PRIVATE cxx_std_20\)/);
  assert.match(androidCmake, /set_target_properties\(jazzrelay PROPERTIES CXX_EXTENSIONS OFF\)/);
  assert.throws(
    () =>
      assert.match(
        androidCmake.replace("cxx_std_20", "cxx_std_17"),
        /target_compile_features\(jazzrelay PRIVATE cxx_std_20\)/,
      ),
    /cxx_std_20/,
    "the Android source receipt must reject lowering the RN target below C++20",
  );
  assert.match(
    androidCmake,
    /\$\{REACT_NATIVE_DIR\}\/ReactAndroid\/src\/main\/jni\/react\/turbomodule/,
  );
  assert.doesNotMatch(androidCmake, /REACT_ANDROID_DIR/);
  assert.doesNotMatch(androidBuild, /generated\/source\/codegen\/java/);
  assert.doesNotMatch(androidBuild, /KotlinCompile/);
  assert.doesNotMatch(androidBuild, /AndroidManifestNew/);
  assert.match(androidBuild, /requires the React Native New Architecture/);
  assert.match(androidPackage, /class JazzRelayPackage/);
  assert.doesNotMatch(androidPackage, /JazzRnModule/);
  assertExactIosModuleRegistration(iosRelay);
  assert.match(iosRelay, /JAZZ_RELAY_ARTIFACT_AVAILABLE/);
  assert.match(iosRelay, /jazz_native_relay_host_execute/);
  assert.match(iosRelay, /<JazzNativeRelay\/jazz_native_relay\.h>/);
  assert.match(iosRelay, /E_JAZZ_RELAY_UNAVAILABLE/);
  assert.match(iosRelay, /NativeJazzRelaySpecJSI/);
  assert.match(iosRelay, /RCT_EXPORT_MODULE\(\)/);
  assert.throws(
    () => assertOpaqueIosRelaySurface(iosRelay.replace("RCT_EXPORT_MODULE()", "")),
    /register exactly once/,
    "the receipt must fail if the iOS TurboModule registration is removed",
  );
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
  assert.match(nativeSpec, /TurboModuleRegistry\.get<Spec>\(["']JazzRelay["']\)/);
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
  assert.match(androidModule, /void initialize\(\)[\s\S]*?bridge\.acquireRuntime\(\)/);
  assert.match(androidModule, /void invalidate\(\)[\s\S]*?bridge\.releaseRuntime\(releasedToken\)/);
  assert.match(iosRelay, /JazzRelayTrustedAdmission/);
  assert.match(iosRelay, /jazz_native_relay_host_admit_scope_json/);
  assert.match(iosRelay, /jazz_native_relay_host_revoke_scope_capability/);
  assert.match(iosRelay, /\[trustedCapabilities addObject:capability\]/);
  assert.match(iosRelay, /\[trustedCapabilities removeObject:capability\]/);
  assert.match(header, /jazz_native_relay_host_admit_scope_json/);
  assert.match(header, /jazz_native_relay_host_revoke_scope_capability/);
});

test("the private foreground JSI host retains teardown ownership and rejects malformed views", async () => {
  const [runtime, header, androidBridge, androidModule, androidCpp, iosRelay] = await Promise.all([
    readFile(
      new URL("../../../crates/jazz-rn/native/foreground-runtime.cpp", import.meta.url),
      "utf8",
    ),
    readFile(
      new URL("../../../crates/jazz-native-relay/include/jazz_native_relay.h", import.meta.url),
      "utf8",
    ),
    readFile(
      new URL(
        "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayBridge.kt",
        import.meta.url,
      ),
      "utf8",
    ),
    readFile(
      new URL(
        "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayModule.java",
        import.meta.url,
      ),
      "utf8",
    ),
    readFile(new URL("../../../crates/jazz-rn/android/cpp-relay.cpp", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/ios/JazzRelay.mm", import.meta.url), "utf8"),
  ]);

  // The HostObjects only call lease APIs while holding the lifecycle lock. A
  // raw host pointer could otherwise be freed after an `active()` observation
  // but before a JS finalizer calls tick/close.
  assert.match(runtime, /lockIfActive\(\)/);
  assert.match(
    runtime,
    /jazz_native_relay_host_lease_invalidate_foreground_runtime\(nativeLease\)/,
    "platform teardown must retire Rust foreground aliases before making JSI finalizers inert",
  );
  assert.match(
    header,
    /jazz_native_relay_host_lease_invalidate_foreground_runtime/,
    "the staged native header must expose the token-scoped teardown ABI",
  );
  assert.match(
    androidBridge,
    /fun acquireRuntime\(\): Long[\s\S]*?activeRuntimeTokens\.add\(token\)/,
    "Android must issue one explicit stable token per JS runtime",
  );
  assert.match(
    androidBridge,
    /fun releaseRuntime\(runtimeToken: Long\)[\s\S]*?nativeInvalidateForegroundRuntime\(host, runtimeToken\)/,
    "Android teardown must invalidate only its own runtime token",
  );
  assert.match(androidModule, /private long runtimeToken = 0;/);
  assert.match(androidModule, /private boolean ownsRuntimeLease = false;/);
  assert.match(androidModule, /bridge\.releaseRuntime\(releasedToken\)/);
  assert.match(
    androidModule,
    /ownsRuntimeLease = false;[\s\S]*?runtimeToken = 0;[\s\S]*?bridge\.releaseRuntime\(releasedToken\)/,
    "Android must clear a module's runtime ownership before release so repeated invalidation cannot consume a sibling lease",
  );
  assert.match(
    androidCpp,
    /using ForegroundRuntimeKey = std::pair<jazz_native_relay_host \*, jlong>;/,
    "Android's native registry must key installations by stable runtime token, not host pointer",
  );
  assert.match(androidCpp, /nativeInvalidateForegroundRuntime\([\s\S]*?jlong runtime_token/);
  assert.doesNotMatch(androidCpp, /unordered_map<jazz_native_relay_host \*/);
  assert.match(iosRelay, /uint64_t foregroundRuntimeToken/);
  assert.match(
    iosRelay,
    /unordered_map<uint64_t, ForegroundRuntimeInstallation>/,
    "iOS must key foreground leases by the explicit runtime token rather than an Objective-C pointer",
  );
  assert.doesNotMatch(
    iosRelay,
    /unordered_map<JazzRelay \*/,
    "iOS source must not rely on libc++ hashing Objective-C object pointers",
  );
  assert.match(
    iosRelay,
    /foregroundRuntimeLeases\.find\(runtimeToken\)/,
    "iOS install and teardown must locate the exact runtime-token lease",
  );
  assert.match(
    iosRelay,
    /found->second\.lease->invalidate\(\);[\s\S]*?foregroundRuntimeLeases\.erase\(found\);[\s\S]*?self\.foregroundRuntimeToken = 0;/,
    "iOS teardown must retire only its module's lease before releasing its runtime token",
  );
  assert.doesNotMatch(iosRelay, /static facebook::jsi::Runtime \*foregroundJsiRuntime/);
  assert.match(runtime, /jazz_native_relay_host_lease_open_attached_foreground/);
  assert.match(runtime, /jazz_native_relay_host_lease_tick_attached_foreground/);
  assert.match(runtime, /jazz_native_relay_host_lease_close_attached_foreground/);
  assert.match(runtime, /jazz_native_relay_host_lease_execute_foreground/);
  assert.match(runtime, /copyForegroundCommand/);
  assert.match(runtime, /foregroundResponse/);
  // The factory's reported ABI must come from the embedded Rust relay, not a
  // second C++ literal. Otherwise an additive foreground command can bump the
  // shared relay ABI while every real JSI factory keeps advertising the old
  // value and the JS wrapper rejects it at installation.
  assert.match(runtime, /jazz_native_relay_abi_version\(\)/);
  assert.doesNotMatch(runtime, /kForegroundAbiVersion/);
  assert.doesNotMatch(runtime, /jazz_native_relay_host_tick_attached_foreground\(lease_->/);

  // Validate floating JSI numbers before narrowing byteOffset to size_t, and
  // reject a DataView/lookalike before copying its selected ArrayBuffer range.
  assert.match(runtime, /getProperty\(runtime, "Uint8Array"\)/);
  assert.match(runtime, /Object::strictEquals/);
  assert.match(runtime, /std::isfinite\(offset_number\)/);
  assert.match(runtime, /std::floor\(offset_number\) != offset_number/);
  assert.match(runtime, /numeric_limits<size_t>::max/);
  assert.throws(
    () => {
      const broken = runtime.replaceAll("std::isfinite(offset_number) ||", "");
      assert.match(broken, /std::isfinite\(offset_number\)/);
    },
    /isfinite/,
    "the receipt is sensitive to removing finite-offset validation",
  );
  assert.throws(
    () => {
      const broken = runtime.replaceAll("Object::strictEquals", "Object::weakEquals");
      assert.match(broken, /Object::strictEquals/);
    },
    /strictEquals/,
    "the receipt is sensitive to accepting non-Uint8Array lookalikes",
  );
  assert.match(header, /jazz_native_relay_host_retain/);
  assert.match(header, /jazz_native_relay_host_lease_free/);
  assert.match(header, /jazz_native_relay_host_lease_execute_foreground/);

  // Wake registration is deliberately a per-foreground JSI lifecycle seam,
  // not another public TurboModule method or foreground command tag. The
  // owner thread can only enqueue CallInvoker work; each JS runtime later
  // drains/ticks itself. These concrete guards make this source receipt
  // sensitive to collapsing wake state across aliases or reintroducing a
  // synchronous callback path.
  assert.match(header, /jazz_native_relay_foreground_wake_callback/);
  assert.match(header, /jazz_native_relay_host_lease_set_foreground_wake_callback/);
  assert.match(runtime, /class ForegroundWakeRegistration/);
  assert.match(runtime, /foreground_/);
  assert.match(runtime, /callbackKey\(\).*foreground_/);
  assert.match(runtime, /CallInvoker/);
  assert.match(runtime, /invokeAsync/);
  assert.match(runtime, /wakeFromOwner\([^)]*\) noexcept/);
  assert.match(runtime, /void schedule\([^)]*\) noexcept/);
  assert.match(runtime, /catch \(\.\.\.\)[\s\S]*scheduled_ = false;/);
  assert.match(runtime, /if \(scheduled_ \|\| !callInvoker_\) return;/);
  assert.match(runtime, /deactivateAndClear/);
  assert.match(runtime, /kWakeCancelled/);
  const openAttached = runtime.match(
    /jazz_native_relay_host_lease_open_attached_foreground\([\s\S]*?Object::createFromHostObject/,
  )?.[0];
  assert.ok(openAttached, "the foreground factory must open through the retained lease");
  assert.match(
    openAttached,
    /lease_lock\.unlock\(\);[\s\S]*Object::createFromHostObject/,
    "ForegroundHandle registration takes the same lifecycle mutex, so construction must happen after the open lock is released",
  );
  assert.throws(
    () => {
      const broken = runtime.replace(
        "if (scheduled_ || !callInvoker_) return;",
        "if (!callInvoker_) return;",
      );
      assert.match(broken, /if \(scheduled_ \|\| !callInvoker_\) return;/);
    },
    /scheduled_/,
    "the receipt is sensitive to removing per-runtime wake coalescing",
  );
  assert.throws(
    () => {
      const broken = runtime.replace("lease_lock.unlock();", "");
      const brokenOpen = broken.match(
        /jazz_native_relay_host_lease_open_attached_foreground\([\s\S]*?Object::createFromHostObject/,
      )?.[0];
      assert.match(brokenOpen, /lease_lock\.unlock\(\);/);
    },
    /unlock/,
    "the receipt is sensitive to reintroducing the foreground-open self-deadlock",
  );
});

test("relay artifact staging targets every supported Android ABI and iOS framework slice", async () => {
  const [script, stagedHeader, sourceHeader] = await Promise.all([
    readFile(
      new URL("../../../crates/jazz-rn/scripts/build-relay-artifacts.sh", import.meta.url),
      "utf8",
    ),
    readFile(
      new URL("../../../crates/jazz-rn/native/include/jazz_native_relay.h", import.meta.url),
      "utf8",
    ),
    readFile(
      new URL("../../../crates/jazz-native-relay/include/jazz_native_relay.h", import.meta.url),
      "utf8",
    ),
  ]);

  assert.equal(
    packageJson.scripts["build:relay:android"],
    "bash scripts/build-relay-artifacts.sh android",
  );
  assert.equal(packageJson.scripts["build:relay:ios"], "bash scripts/build-relay-artifacts.sh ios");
  assert.match(script, /\[arm64-v8a\]=aarch64-linux-android/);
  assert.match(script, /\[armeabi-v7a\]=armv7-linux-androideabi/);
  assert.match(script, /\[x86_64\]=x86_64-linux-android/);
  assert.doesNotMatch(script, /i686-linux-android|\[x86\]/);
  assert.match(script, /JazzNativeRelay\.xcframework/);
  assert.match(script, /aarch64-apple-ios-sim x86_64-apple-ios/);
  assert.match(script, /simulator_stage=.*simulator/);
  assert.match(script, /\$simulator_stage\/libjazz_native_relay\.a/);
  assert.match(script, /nativeRelayAbi/);
  assert.match(script, /cp "\$root\/crates\/jazz-native-relay\/include\/jazz_native_relay\.h"/);
  assert.equal(
    stagedHeader,
    sourceHeader,
    "the checked-in package header is a byte-for-byte copy of the authoritative relay ABI header",
  );
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
      execFileSync("npm", npmPackMachineArgs("--dry-run"), {
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
    /pub const NATIVE_RELAY_ABI_V1: u16 = (\d+);/.exec(
      readFileSync(
        new URL("../../../crates/jazz-native-relay/src/lib.rs", import.meta.url),
        "utf8",
      ),
    )?.[1],
  );
  const nativeSourceInventory = execFileSync(
    "git",
    [
      "ls-tree",
      "-r",
      "--full-tree",
      "HEAD",
      "--",
      "Cargo.lock",
      "Cargo.toml",
      "crates/groove",
      "crates/idb-tree",
      "crates/jazz",
      "crates/jazz-compression",
      "crates/jazz-native-relay",
      "crates/jazz-storage-sqlite",
      "crates/jazz-rn/scripts/build-relay-artifacts.sh",
    ],
    { cwd: sourceRoot, encoding: "utf8" },
  );
  const fingerprintNativeSource = (inventory) =>
    createHash("sha256").update(inventory).digest("hex");
  const nativeSourceFingerprint = fingerprintNativeSource(nativeSourceInventory);
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
        { format: 2, nativeRelayAbi, sourceRevision, nativeSourceFingerprint, ...extra, files },
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

    for (const transitiveInput of ["crates/idb-tree/", "crates/jazz-compression/"]) {
      const entry = nativeSourceInventory
        .split("\n")
        .find((line) => line.includes(transitiveInput));
      assert.ok(entry, `native source inventory must contain ${transitiveInput}`);
      const staleFingerprint = fingerprintNativeSource(
        nativeSourceInventory.replace(entry, `${entry} planted-change`),
      );
      assert.notEqual(
        staleFingerprint,
        nativeSourceFingerprint,
        `a ${transitiveInput} source change must change the native fingerprint`,
      );
      await writeManifest(
        androidRoot,
        join(packageRoot, "android/jazz-native-relay.manifest.json"),
        {
          toolchain: { cargoNdk: "4.1.2" },
          nativeSourceFingerprint: staleFingerprint,
        },
      );
      assert.throws(
        () =>
          execFileSync(
            process.execPath,
            [verifier.pathname, "--package-root", packageRoot, "android", "ios"],
            { env: environment, stdio: "pipe" },
          ),
        /native source fingerprint/,
        `a sealed archive built before a ${transitiveInput} source change is not releasable`,
      );
    }
    await writeManifest(androidRoot, join(packageRoot, "android/jazz-native-relay.manifest.json"), {
      toolchain: { cargoNdk: "4.1.2" },
    });

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
  for (const [reuse, expected] of [
    [true, previewCommit],
    [false, mergeCommit],
  ]) {
    assert.equal(select(reuse), expected, `reuse=${reuse} selects the sealed source revision`);
  }
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

test("both alpha jazz-rn verifiers use the shared release source-revision contract", async () => {
  const workflowText = await readFile(
    new URL("../../../.github/workflows/publish-jazz-tools-alpha.yml", import.meta.url),
    "utf8",
  );
  const workflow = parse(workflowText);
  const steps = workflow.jobs["publish-npm"].steps;
  const staged = steps.find((step) => step.name === "Verify staged jazz-rn relay artifacts");
  const packed = steps.find((step) => step.name === "Verify packed jazz-rn relay payload");

  assert.ok(staged, "the staged relay verifier must remain in the alpha release workflow");
  assert.ok(packed, "the packed relay verifier must remain in the alpha release workflow");
  assert.deepEqual(
    packed.env,
    staged.env,
    "the packed verifier must receive the same reuse-aware source selector inputs as staged verification",
  );
  for (const [name, step] of [
    ["staged", staged],
    ["packed", packed],
  ]) {
    assert.match(
      step.run,
      /JAZZ_NATIVE_RELAY_SOURCE_REVISION="\$\(node dev\/artifacts\/release-artifact-source-revision\.mjs\)"/,
      `${name} verification must use the authoritative release source selector`,
    );
  }
  assert.doesNotMatch(
    packed.run,
    /git rev-parse HEAD/,
    "packed verification must not replace a reusable preview's sealed revision with the merge SHA",
  );
  assert.match(
    workflowText,
    /env: &release-artifact-source-revision-env[\s\S]*env: \*release-artifact-source-revision-env/,
    "the two verifier sites must share one env definition so their reuse inputs cannot drift",
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
  for (const nativeInput of [
    "Cargo.lock",
    "Cargo.toml",
    "crates/groove",
    "crates/idb-tree",
    "crates/jazz",
    "crates/jazz-compression",
    "crates/jazz-native-relay",
    "crates/jazz-storage-sqlite",
  ]) {
    assert.ok(artifactScript.includes(nativeInput), `artifact fingerprint omits ${nativeInput}`);
    assert.ok(verifier.includes(nativeInput), `artifact verifier omits ${nativeInput}`);
    assert.ok(rnWorkflow.includes(nativeInput), `native workflow receipt omits ${nativeInput}`);
  }
  assert.match(packageBuild, /cargo-ndk@\$\{\{ env\.JAZZ_RN_CARGO_NDK_VERSION \}\}/);
  assert.match(packageBuild, /--package-root/);
  assert.match(verifier, /relay artifact inventory differs from its manifest/);
});
