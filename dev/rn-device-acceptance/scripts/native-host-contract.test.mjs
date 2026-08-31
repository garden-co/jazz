import assert from "node:assert/strict";
import fs from "node:fs";
import { execFileSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import ts from "typescript";
import { DEVICE_DIAGNOSTIC_CODES } from "../src/device-diagnostics.ts";

const root = path.resolve(import.meta.dirname, "..");
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");

// Documentation is part of the native-package contract. Normalize prose
// formatting so legitimate wrapping/Markdown emphasis edits do not turn this
// into a brittle snapshot, while materially stronger or weaker claims fail.
const prose = (text) => text.replace(/[`*]/g, "").replace(/\s+/g, " ").trim();

function assertCurrentRnBoundary(packageReadme, installGuide, spec) {
  assert.match(
    packageReadme,
    /narrow alpha rather than general React Native support/,
    "the package must not claim broad RN support",
  );
  assert.match(
    packageReadme,
    /matching native development or release build.*capability issued by trusted platform admission/i,
    "persistent foreground use must retain both native-build and trusted-admission requirements",
  );
  assert.match(
    packageReadme,
    /two physical JSI runtimes.*remains pending/i,
    "same-runtime alias coverage must not be advertised as a multi-runtime device proof",
  );
  assert.match(
    installGuide,
    /narrow, capability-gated foreground alpha.*matching native development\/release build.*trusted platform admission/i,
    "the public install guide must describe the same constrained boundary",
  );
  assert.match(
    spec,
    /physical JSI runtime.*pending/i,
    "the normative implementation sequence must retain the missing physical-runtime proof",
  );
  assert.doesNotMatch(
    packageReadme,
    /high-level jazz client.*under restoration|modules deliberately report that the Rust relay artifact is unavailable/i,
    "removed pre-foreground claims must not survive in the packaging README",
  );
}

function jobSection(workflow, job) {
  const escapedJob = job.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const section = new RegExp(
    `^  ${escapedJob}:\\n([\\s\\S]*?)(?=^  [A-Za-z0-9_-]+:|(?![\\s\\S]))`,
    "m",
  ).exec(workflow)?.[1];
  assert.ok(section, `workflow is missing ${job}`);
  return section;
}

function jobIfCondition(workflow, job) {
  const section = jobSection(workflow, job);
  const condition = /^    if: >-\n([\s\S]*?)(?=^    (?:name|runs-on):)/m.exec(section)?.[1];
  assert.ok(condition, `${job} must have a block scalar if condition before name/runs-on`);
  return condition.replace(/\s+/g, " ").trim();
}

function assertRnDeviceWorkflowContract(workflow) {
  const ios = jobIfCondition(workflow, "ios-simulator");
  const android = jobIfCondition(workflow, "android-device-acceptance");
  assert.equal(
    android,
    ios,
    "Android and iOS native device jobs must use one identical opt-in contract",
  );
  assert.match(
    android,
    /github\.event_name == 'workflow_dispatch' \|\| \( contains\(github\.event\.pull_request\.labels\.\*\.name, 'rn-preview-release'\) && github\.event\.pull_request\.head\.repo\.full_name == github\.repository \)/,
    "native device jobs run only when manually dispatched or for a same-repository RN-preview PR",
  );
}

function assertAtomicAndroidDiagnostic(fixture) {
  assert.match(fixture, /private fun writeAtomicDiagnostic\(code: String\)/);
  assert.match(fixture, /File\.createTempFile\("\.\$\{target\.name\}\."/);
  assert.match(fixture, /FileOutputStream\(temporary\)\.use/);
  assert.match(fixture, /output\.fd\.sync\(\)/);
  assert.match(fixture, /Os\.rename\(temporary\.absolutePath, target\.absolutePath\)/);
  assert.match(fixture, /temporary\.exists\(\) && !temporary\.delete\(\)/);
  assert.doesNotMatch(
    fixture,
    /resolve\("jazz-device-diagnostic\.txt"\)\.writeText\(code\)/,
    "the host must never inspect a torn direct diagnostic write",
  );
}

function extractAndroidDiagnosticCodes(fixture) {
  const body = /private val diagnosticCodes = setOf\(([\s\S]*?)\n  \)/.exec(fixture)?.[1];
  assert.ok(body, "Android fixture must declare its diagnostic allowlist");
  return [...body.matchAll(/^\s*"([^"]+)",?$/gm)].map((match) => match[1]);
}

function extractIosDiagnosticCodes(fixture) {
  const body =
    /static NSSet<NSString \*> \*JazzDeviceDiagnosticCodes\(void\) \{[\s\S]*?setWithArray:@\[([\s\S]*?)\n  \]\];/.exec(
      fixture,
    )?.[1];
  assert.ok(body, "iOS fixture must declare its diagnostic allowlist");
  return [...body.matchAll(/^\s*@"([^"]+)",?$/gm)].map((match) => match[1]);
}

function assertOrderedDiagnosticCodes(fixture, platform) {
  const actual =
    platform === "android"
      ? extractAndroidDiagnosticCodes(fixture)
      : extractIosDiagnosticCodes(fixture);
  assert.deepEqual(
    actual,
    DEVICE_DIAGNOSTIC_CODES,
    `${platform} fixture diagnostic allowlist must exactly follow the shared order`,
  );
}

function swapSubscriptionWriteStages(fixture) {
  return fixture
    .replace("same-runtime-transaction-open-failed", "__diagnostic-stage-swap__")
    .replace("same-runtime-mutation-stage-failed", "same-runtime-transaction-open-failed")
    .replace("__diagnostic-stage-swap__", "same-runtime-mutation-stage-failed");
}

function assertPublicClientSeedStages(source) {
  const stages = ["open", "subscribe", "write", "read", "publish", "shutdown"];
  let previous = -1;
  for (const stage of stages) {
    const marker = `markFailure("public-client-${stage}-failed")`;
    const position = source.indexOf(marker);
    assert.ok(position >= 0, `public client seed is missing its ${stage} stage`);
    assert.ok(position > previous, `public client seed stage ${stage} is out of order`);
    previous = position;
  }
  for (const pattern of [
    /markFailure\("public-client-open-failed"\);\s*const client = await createJazzClient/,
    /markFailure\("public-client-subscribe-failed"\);\s*unsubscribe = client\.db\.subscribe/,
    /markFailure\("public-client-write-failed"\);\s*const write = client\.db\.insert/,
    /markFailure\("public-client-read-failed"\);\s*const rows = await client\.db\.all/,
    /markFailure\("public-client-publish-failed"\);\s*if \(!\(await waitForPublication\(\(\) => observed\)\)\)/,
    /if \(completed && !failed\) markFailure\("public-client-shutdown-failed"\);\s*await finishSeedClient/,
  ]) {
    assert.match(source, pattern, "public client seed stage moved away from its native boundary");
  }
}

function isIdentifier(node, text) {
  return ts.isIdentifier(node) && node.text === text;
}

function isPropertyAccess(node, object, property) {
  return (
    node &&
    ts.isPropertyAccessExpression(node) &&
    isIdentifier(node.expression, object) &&
    node.name.text === property
  );
}

function relayReadbackFunction(source) {
  const file = ts.createSourceFile(
    "high-level-foreground.ts",
    source,
    ts.ScriptTarget.Latest,
    true,
  );
  assert.deepEqual(file.parseDiagnostics, [], "high-level foreground source must parse");
  const matches = file.statements.filter(
    (statement) =>
      ts.isFunctionDeclaration(statement) &&
      statement.name?.text === "proveHighLevelForegroundRelayReadback",
  );
  assert.equal(
    matches.length,
    1,
    "relay readback must have exactly one named function declaration",
  );
  const fn = matches[0];
  assert.ok(fn.body, "relay readback must have a function body");
  return { file, fn };
}

function relayReadbackSlice(source) {
  const { file, fn } = relayReadbackFunction(source);
  return source.slice(fn.getStart(file), fn.end);
}

function awaitCall(expression) {
  return ts.isAwaitExpression(expression) && ts.isCallExpression(expression.expression)
    ? expression.expression
    : undefined;
}

function variableDeclaration(statement, name) {
  if (!ts.isVariableStatement(statement)) return undefined;
  return statement.declarationList.declarations.find((declaration) =>
    isIdentifier(declaration.name, name),
  );
}

/** Parse the actual TypeScript AST so comments, strings, templates, and regex
 * literals cannot stand in for executable calls. This is a fixed harness, so
 * its function body intentionally has only client creation then lifecycle;
 * the lifecycle has only public read then assertion; and finally has only
 * shutdown. */
function assertPublicClientRelayReadback(source) {
  const { fn } = relayReadbackFunction(source);
  const statements = fn.body.statements;
  assert.equal(
    statements.length,
    2,
    "relay readback function body must contain exactly client creation then try/finally",
  );
  const [clientStatement, lifecycle] = statements;
  assert.ok(
    ts.isVariableStatement(clientStatement) &&
      (clientStatement.declarationList.flags & ts.NodeFlags.Const) !== 0 &&
      clientStatement.declarationList.declarations.length === 1,
    "relay readback must create client as one const declaration",
  );
  const client = variableDeclaration(clientStatement, "client");
  assert.ok(client?.initializer, "relay readback must create a fresh public client");
  const create = awaitCall(client.initializer);
  assert.ok(
    create &&
      isIdentifier(create.expression, "createJazzClient") &&
      create.arguments.length === 1 &&
      ts.isCallExpression(create.arguments[0]) &&
      isIdentifier(create.arguments[0].expression, "clientConfig") &&
      create.arguments[0].arguments.length === 1 &&
      isIdentifier(create.arguments[0].arguments[0], "capability"),
    "relay readback must await createJazzClient(clientConfig(capability))",
  );

  assert.ok(
    ts.isTryStatement(lifecycle),
    "relay readback must use try/finally after client creation",
  );
  assert.equal(
    lifecycle.catchClause,
    undefined,
    "relay readback must not swallow lifecycle failures",
  );
  assert.ok(lifecycle.finallyBlock, "relay readback must shut down in finally");
  const tryStatements = lifecycle.tryBlock.statements;
  assert.equal(
    tryStatements.length,
    2,
    "relay readback try block must contain exactly public read then persisted-title assertion",
  );
  const [rowsStatement, assertionStatement] = tryStatements;
  assert.ok(
    ts.isVariableStatement(rowsStatement) &&
      (rowsStatement.declarationList.flags & ts.NodeFlags.Const) !== 0 &&
      rowsStatement.declarationList.declarations.length === 1,
    "relay readback must bind rows as one const declaration",
  );
  const rows = variableDeclaration(rowsStatement, "rows");
  assert.ok(rows?.initializer, "relay readback must bind rows from a public read");
  const read = awaitCall(rows.initializer);
  assert.ok(
    read &&
      ts.isPropertyAccessExpression(read.expression) &&
      read.expression.name.text === "all" &&
      isPropertyAccess(read.expression.expression, "client", "db") &&
      read.arguments.length === 1 &&
      isPropertyAccess(read.arguments[0], "app", "todos"),
    "relay readback must await client.db.all(app.todos) into rows",
  );

  assert.ok(
    ts.isExpressionStatement(assertionStatement) &&
      ts.isCallExpression(assertionStatement.expression) &&
      isIdentifier(assertionStatement.expression.expression, "assertPersistedTitleForRun"),
    "relay readback must immediately assert its public rows",
  );
  const assertion = assertionStatement;
  const call = assertion.expression;
  assert.equal(
    call.arguments.length,
    2,
    "persisted-title assertion must receive rows and run nonce",
  );
  const [titles, nonce] = call.arguments;
  assert.ok(
    ts.isCallExpression(titles) &&
      isPropertyAccess(titles.expression, "rows", "map") &&
      titles.arguments.length === 1 &&
      ts.isArrowFunction(titles.arguments[0]) &&
      titles.arguments[0].parameters.length === 1 &&
      isIdentifier(titles.arguments[0].parameters[0].name, "row") &&
      isPropertyAccess(titles.arguments[0].body, "row", "title") &&
      isIdentifier(nonce, "runNonce"),
    "relay readback must derive the persisted-title assertion from rows.map(row => row.title) and runNonce",
  );

  assert.equal(
    lifecycle.finallyBlock.statements.length,
    1,
    "relay readback finally must contain only client shutdown",
  );
  const [shutdown] = lifecycle.finallyBlock.statements;
  assert.ok(
    ts.isExpressionStatement(shutdown) &&
      awaitCall(shutdown.expression)?.arguments.length === 0 &&
      isPropertyAccess(awaitCall(shutdown.expression)?.expression, "client", "shutdown"),
    "relay readback must await client.shutdown() in finally",
  );
  assert.ok(client.pos < rows.pos, "public client creation must precede its public read");
  assert.ok(rows.pos < assertion.pos, "public read must precede the persisted-title assertion");
  assert.ok(assertion.pos < shutdown.pos, "persisted-title assertion must precede final shutdown");
}

test("Android fixture BuildConfig fields and package registration remain compile-shaped", () => {
  const gradle = read("android/app/build.gradle");
  const fixture = read(
    "android/app/src/main/java/dev/jazz/rndeviceacceptance/JazzDeviceFixtureModule.kt",
  );
  assert.equal(
    fixture,
    read("native/android/JazzDeviceFixtureModule.kt"),
    "the checked-in Android host fixture must match the prebuild template",
  );
  const registration = read(
    "android/app/src/main/java/dev/jazz/rndeviceacceptance/JazzDeviceFixturePackage.kt",
  );
  const host = read("android/app/src/main/java/dev/jazz/rndeviceacceptance/MainApplication.kt");
  for (const field of [
    "APP_NAMESPACE",
    "STORAGE_NAMESPACE",
    "AUTH_SCOPE",
    "SCHEMA_JSON",
    "VERIFIED_CLAIMS_JSON",
  ]) {
    assert.match(gradle, new RegExp(`buildConfigField "String", "JAZZ_DEVICE_${field}"`));
    assert.match(fixture, new RegExp(`BuildConfig\\.JAZZ_DEVICE_${field}`));
  }
  assert.match(gradle, /JAZZ_DEVICE_SCHEMA_JSON.*todos/);
  assert.match(registration, /class JazzDeviceFixturePackage : ReactPackage/);
  assert.match(registration, /listOf\(JazzDeviceFixtureModule\(context\)\)/);
  assert.match(host, /add\(JazzDeviceFixturePackage\(\)\)/);
  assert.match(fixture, /Build\.FINGERPRINT/);
  assert.match(fixture, /scopeConfig\(authScope: String\)/);
  assert.match(fixture, /fixture-user-b/);
  assert.match(fixture, /JazzRelayTrustedAdmission\.replace/);
  assert.match(fixture, /jazz-device-\$authScope\.sqlite/);
  assert.match(fixture, /jazzDeviceRunNonce/);
  assert.match(fixture, /applicationInfo\.sourceDir/);
  assert.match(fixture, /MessageDigest\.getInstance\("SHA-256"\)/);
  assert.match(fixture, /@ReactMethod fun recordReceipt/);
  assert.match(fixture, /jazz-device-receipt\.ndjson/);
  assert.match(fixture, /@ReactMethod fun recordDiagnostic/);
  assert.match(fixture, /require\(code in diagnosticCodes\)/);
  for (const candidate of [fixture, read("native/android/JazzDeviceFixtureModule.kt")]) {
    assertOrderedDiagnosticCodes(candidate, "android");
    assert.throws(
      () => assertOrderedDiagnosticCodes(swapSubscriptionWriteStages(candidate), "android"),
      /must exactly follow the shared order/,
      "swapping two codes must fail even when generated and checked-in fixtures agree",
    );
  }
  assert.match(fixture, /jazz-device-diagnostic\.txt/);
  assert.match(fixture, /Log\.e\("JazzDeviceAcceptance", code\)/);
  assertAtomicAndroidDiagnostic(fixture);
  // A tempting direct write makes a timeout expose torn bytes. This plant
  // proves the contract rejects it even though the rest of the fixture stays
  // compile-shaped.
  assert.throws(
    () =>
      assertAtomicAndroidDiagnostic(
        fixture.replace(
          "writeAtomicDiagnostic(code)",
          'reactApplicationContext.cacheDir.resolve("jazz-device-diagnostic.txt").writeText(code)',
        ),
      ),
    /torn direct diagnostic write/,
  );
  assert.doesNotMatch(fixture, /jazzDeviceBuildFingerprint/);
});

test("iOS fixture imports the public JazzRn pod header, not its private relay framework", () => {
  const podspec = fs.readFileSync(
    path.resolve(root, "../../crates/jazz-rn/JazzRn.podspec"),
    "utf8",
  );
  const fixture = read("native/ios/JazzDeviceFixture.mm");
  assert.match(podspec, /s\.name\s+=\s+"JazzRn"/);
  assert.match(podspec, /s\.source_files\s+=\s+"ios\/\*\*\/\*\.\{h,m,mm,swift\}"/);
  assert.match(fixture, /#import <JazzRn\/JazzRelay\.h>/);
  assert.doesNotMatch(fixture, /JazzNativeRelay\/JazzRelay\.h/);
  assert.equal(
    fixture,
    read("ios/JazzDeviceFixture.mm"),
    "the checked-in iOS host fixture must match the prebuild template",
  );
});

test("Android acceptance reads only bounded receipt and allowlisted diagnostic tags", () => {
  const driver = read("scripts/run-android.mjs");
  assert.match(
    driver,
    /"logcat",[\s\S]*"-d",[\s\S]*"-v",[\s\S]*"threadtime",[\s\S]*"ReactNativeJS:I",[\s\S]*"JazzDeviceAcceptance:E",[\s\S]*"\*:S"/,
  );
  assert.doesNotMatch(driver, /adb\(\["logcat", "-d"\]\)/);
  assert.match(driver, /adb\(\["logcat", "-c"\]\)/);
  assert.ok(
    driver.indexOf('adb(["logcat", "-c"])') <
      driver.indexOf('"dev.jazz.rndeviceacceptance\/.MainActivity"'),
    "each launched phase must clear stale log diagnostics before starting its app process",
  );
  assert.match(driver, /androidAcceptanceFailure\("invalid-receipt", phase, output\)/);
  assert.match(driver, /androidAcceptanceFailure\("timeout", phase, output\)/);
});

test("each native JSI runtime owns an independent foreground lease", () => {
  const androidBridge = fs.readFileSync(
    path.resolve(root, "../../crates/jazz-rn/android/cpp-relay.cpp"),
    "utf8",
  );
  const iosBridge = fs.readFileSync(
    path.resolve(root, "../../crates/jazz-rn/ios/JazzRelay.mm"),
    "utf8",
  );
  // Android's Kotlin wrapper already allocates a runtime token. This receipt
  // fails if JNI silently drops it and returns to one host-global lease.
  assert.match(androidBridge, /ForegroundRuntimeKey = std::pair<jazz_native_relay_host \*, jlong>/);
  assert.match(androidBridge, /nativeForegroundBindingsInstaller\([\s\S]*jlong runtime_token\)/);
  assert.match(androidBridge, /foregroundInstallation\(relay_host, runtime_token, callInvoker\)/);
  assert.match(androidBridge, /foreground_installations\.find\(\{relay_host, runtime_token\}\)/);
  assert.doesNotMatch(
    androidBridge,
    /std::unordered_map<jazz_native_relay_host \*, std::shared_ptr<ForegroundRuntimeInstallation>>/,
  );
  // iOS uses an opaque, monotonically allocated runtime token instead of an
  // Objective-C pointer identity. A pointer can be recycled after teardown;
  // the token is never reused and is also checked by the native lease. A
  // planted process-global `foregroundRuntimeLease` would make either bridge
  // teardown kill its sibling and fails this source/device-host receipt.
  assert.match(iosBridge, /static uint64_t nextForegroundRuntimeToken = 1/);
  assert.match(iosBridge, /std::unordered_map<uint64_t, ForegroundRuntimeInstallation>/);
  assert.match(iosBridge, /foregroundRuntimeLeases\.find\(runtimeToken\)/);
  assert.match(iosBridge, /foregroundRuntimeLeases\.erase\(found\)/);
  assert.doesNotMatch(iosBridge, /std::unordered_map<JazzRelay \*, ForegroundRuntimeInstallation>/);
  assert.doesNotMatch(
    iosBridge,
    /static std::shared_ptr<jazz::rn::ForegroundRuntimeLease> foregroundRuntimeLease/,
  );
});

test("foreground wake lifecycle clears native callbacks before close or runtime revocation", () => {
  const foregroundRuntime = fs.readFileSync(
    path.resolve(root, "../../crates/jazz-rn/native/foreground-runtime.cpp"),
    "utf8",
  );
  // A CallInvoker task may already be queued when the JS alias closes. The
  // registration must first become inactive, clear the Rust callback, and the
  // delayed task must re-check active state before it looks up JS values.
  assert.match(foregroundRuntime, /active_ = false;[\s\S]*pending_ = false;/);
  assert.match(
    foregroundRuntime,
    /jazz_native_relay_host_lease_set_foreground_wake_callback\([\s\S]*nullptr, nullptr\)/,
  );
  assert.match(foregroundRuntime, /if \(!active_ \|\| !pending_\) return;/);
  assert.match(foregroundRuntime, /if \(kind == kWakeCancelled\) \{[\s\S]*active_ = false;/);
  assert.match(foregroundRuntime, /catch \(\.\.\.\) \{[\s\S]*scheduled_ = false;/);
  // Native handle close/revoke reaches the same clear-before-free operation;
  // a late callback cannot retain a stale JSI function or escape as unhandled.
  assert.match(foregroundRuntime, /wake_->deactivateAndClear\(lease_->nativeLease\(\)\);/);
  assert.match(foregroundRuntime, /wake_->removeCallback\(runtime\);/);
});

test("Expo config plugin describes the real iOS receipt boundary without claiming TODO scenarios", () => {
  const plugin = read("plugins/with-jazz-device-fixture.cjs");
  assert.match(plugin, /JAZZ_DEVICE_SCHEMA_JSON.*todos/);
  assert.match(plugin, /label-gated iOS simulator workflow/);
  assert.match(plugin, /requires its linked/);
  assert.match(plugin, /ABI\/admission receipt/);
  assert.match(plugin, /Multi-peer acceptance remains TODO \(#2291\)/);
  assert.doesNotMatch(plugin, /this workflow is source-only/);
});

test("RN packaging and public docs describe the proven alpha boundary semantically", () => {
  const packageReadme = prose(
    fs.readFileSync(path.resolve(root, "../../crates/jazz-rn/README.md"), "utf8"),
  );
  const installGuide = prose(
    fs.readFileSync(path.resolve(root, "../../docs/content/docs/install/client.mdx"), "utf8"),
  );
  const spec = prose(
    fs.readFileSync(path.resolve(root, "../../crates/jazz/SPEC/19_native_relays.md"), "utf8"),
  );
  assertCurrentRnBoundary(packageReadme, installGuide, spec);

  // Plant the tempting but false extrapolation from two aliases in one JSI
  // runtime to two physical runtimes. The semantic receipt must reject it.
  assert.throws(
    () =>
      assertCurrentRnBoundary(
        packageReadme.replace("two physical JSI runtimes", "two aliases in one JSI runtime"),
        installGuide,
        spec,
      ),
    /same-runtime alias coverage/i,
  );
});

test("device fixture does not import internal jazz-tools relay-frame types", () => {
  const fixture = read("src/native-fixture.ts");
  assert.doesNotMatch(fixture, /NativeRelay(?:Capability|Executor)/);
  assert.match(fixture, /executor: \{ execute: executeNativeRelayCommand \}/);
});

test("protocol receipt builds its workspace API prerequisites from clean outputs", () => {
  const acceptancePackage = JSON.parse(read("package.json"));
  const jazzToolsPackage = JSON.parse(
    fs.readFileSync(path.resolve(root, "../../packages/jazz-tools/package.json"), "utf8"),
  );
  const protocol = acceptancePackage.scripts["test:protocol"];
  const workspacePrerequisites = acceptancePackage.scripts["build:workspace-prerequisites"];
  const prerequisites = "pnpm build:workspace-prerequisites";

  // `jazz-tools/react-native` is a generated public entrypoint. The receipt
  // must create it itself rather than accidentally relying on artifacts left
  // by another root Turbo task or an earlier CI step. The RN public entry is
  // TypeScript-only: it must not pull in the browser worker/WASM bundle.
  assert.match(
    protocol,
    new RegExp(`^${prerequisites.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")} && `),
  );
  assert.equal(
    workspacePrerequisites,
    "pnpm --filter jazz-tools build:react-native && pnpm --filter jazz-rn build",
    "the receipt must stage the narrow RN public entrypoint, not Jazz Tools' browser/WASM build",
  );
  assert.equal(jazzToolsPackage.scripts["build:react-native"], "tsc --project tsconfig.json");
  assert.doesNotMatch(
    jazzToolsPackage.scripts["build:react-native"],
    /bundle-broker-worker|jazz-wasm/,
  );

  // Plant the former stale-artifact shortcut: skipping the prerequisite stage
  // must not satisfy the receipt, even though a developer's checkout may
  // happen to retain `dist/react-native` from an earlier build.
  assert.throws(
    () =>
      assert.match(
        protocol.replace("pnpm build:workspace-prerequisites && ", ""),
        /workspace-prerequisites/,
      ),
    /workspace-prerequisites/,
  );
  assert.throws(
    () =>
      assert.match(
        workspacePrerequisites.replace("jazz-tools build:react-native", "jazz-tools build"),
        /jazz-tools build:react-native/,
      ),
    /jazz-tools build:react-native/,
    "a planted full browser/WASM build must fail the RN-only prerequisite contract",
  );
});

test("the React Native public entrypoint cannot reach the browser WASM loader", () => {
  const runtimeSource = fs.readFileSync(
    path.resolve(root, "../../packages/jazz-tools/src/react-native/runtime-source.ts"),
    "utf8",
  );
  const browserRuntimeLoader = fs.readFileSync(
    path.resolve(root, "../../packages/jazz-tools/src/runtime/default-runtime-source.ts"),
    "utf8",
  );

  // Metro resolves the complete static module graph while bundling. A fallback
  // which only *executes* in a memory configuration still makes the native
  // package depend on the browser `jazz-wasm` artifact. RN intentionally has
  // one runtime: the installed native relay.
  const assertNoBrowserRuntime = (source) => {
    assert.doesNotMatch(source, /^\s*import[^\n]*DefaultRuntimeSource/m);
    assert.doesNotMatch(source, /^\s*import[^\n]*wasm-loader/m);
    assert.doesNotMatch(source, /^\s*import[^\n]*jazz-wasm/m);
    assert.match(source, /REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR/);
  };
  assertNoBrowserRuntime(runtimeSource);
  assert.match(browserRuntimeLoader, /wasm-loader/);

  // Plant the historical fallback. The receipt must fail rather than relying
  // on a successful TypeScript build to notice Metro's transitive resolution.
  assert.throws(
    () =>
      assertNoBrowserRuntime(
        `${runtimeSource}\nimport { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";`,
      ),
    /DefaultRuntimeSource/,
  );
});

function emittedRelativeModuleGraph(entry, sourceOverrides = new Map()) {
  const pending = [entry];
  const visited = new Map();
  const staticSpecifiers =
    /(?:^|\n)\s*(?:import|export)\s+(?:[^"'\n]*?\s+from\s+)?["']([^"']+)["']/g;
  const dynamicSpecifiers = /\bimport\(\s*["']([^"']+)["']\s*\)/g;

  while (pending.length > 0) {
    const file = pending.pop();
    if (!file || visited.has(file)) continue;
    const source = sourceOverrides.get(file) ?? fs.readFileSync(file, "utf8");
    visited.set(file, source);
    for (const matcher of [staticSpecifiers, dynamicSpecifiers]) {
      matcher.lastIndex = 0;
      for (const match of source.matchAll(matcher)) {
        const specifier = match[1];
        if (!specifier?.startsWith(".")) continue;
        const target = path.resolve(path.dirname(file), specifier);
        assert.equal(fs.existsSync(target), true, `emitted relative module must exist: ${target}`);
        pending.push(target);
      }
    }
  }
  return visited;
}

function assertEmittedRnGraphHasNoBrowserWasm(graph) {
  const names = [...graph.keys()].map((file) => file.replaceAll("\\", "/"));
  assert.equal(
    names.some((file) => /\/runtime\/(?:wasm-loader|default-runtime-source)\.js$/.test(file)),
    false,
    "the emitted RN graph must not reach browser/Node runtime loading",
  );
  assert.equal(
    [...graph.values()].some((source) => /(?:from\s*|import\()\s*["']jazz-wasm["']/.test(source)),
    false,
    "the emitted RN graph must not mention the browser WASM package",
  );
}

test("the emitted React Native graph cannot reach browser WASM through barrel exports", () => {
  const entry = path.resolve(root, "../../packages/jazz-tools/dist/react-native/index.js");
  const graph = emittedRelativeModuleGraph(entry);
  assertEmittedRnGraphHasNoBrowserWasm(graph);

  // Plant the former `../index.js` re-export. This follows the emitted barrel
  // graph, unlike a direct source-import scan, and must discover its WASM
  // loader through runtime/index.
  const oldBarrel = fs.readFileSync(entry, "utf8");
  const planted = new Map([[entry, oldBarrel.replace("../schema-namespace.js", "../index.js")]]);
  assert.throws(
    () => assertEmittedRnGraphHasNoBrowserWasm(emittedRelativeModuleGraph(entry, planted)),
    /browser\/Node runtime loading|browser WASM package/,
  );
});

test("process-restart acceptance has two disjoint, host-terminated phases", () => {
  const androidDriver = read("scripts/run-android.mjs");
  const iosDriver = read("scripts/run-ios.mjs");
  const app = read("App.tsx");
  const highLevelForeground = read("src/high-level-foreground.ts");
  const scenarios = read("src/scenarios.ts");
  const androidFixture = read("native/android/JazzDeviceFixtureModule.kt");
  const iosFixture = read("native/ios/JazzDeviceFixture.mm");

  for (const driver of [androidDriver, iosDriver]) {
    assert.match(driver, /launchAndAssert\("seed"\)/);
    assert.match(driver, /launchAndAssert\("verify"\)/);
    assert.match(driver, /scenariosForAcceptancePhase\(phase\)/);
  }
  assert.match(androidDriver, /am", "force-stop", "dev\.jazz\.rndeviceacceptance/);
  assert.match(iosDriver, /simctl\(\["terminate", udid, "dev\.jazz\.rndeviceacceptance"\]\)/);
  assert.match(scenarios, /phase === "verify"\s*\?\s*scenario\.scenario === "reopen"/);
  // The restart assertion must cross the public RN API. A byte-level `all`
  // after re-admission only proves the host transport; it does not prove a
  // fresh application can start, select its foreground runtime, and decode a
  // persisted row through `createJazzClient`.
  assert.match(
    app,
    /await proveHighLevelForegroundRestart\(reopened\.capability, receipt\.runNonce\)/,
  );
  assert.match(
    app,
    /seedHighLevelForegroundRuntime\(scopeA\.capability, receipt\.runNonce, markFailure\)/,
  );
  // Before the driver ends the seed process, a separately opened public
  // foreground must read the run-bound row. This prevents the restart claim
  // from depending solely on the writer's in-memory foreground preview.
  assert.match(
    app,
    /markFailure\("public-client-relay-readback-failed"\);\s*await proveHighLevelForegroundRelayReadback\(scopeA\.capability, receipt\.runNonce\)/,
  );
  assert.match(highLevelForeground, /createJazzClient\(clientConfig\(capability\)\)/);
  assert.match(highLevelForeground, /client\.db\.all\(app\.todos\)/);
  assert.match(highLevelForeground, /assertPersistedTitleForRun/);
  assertPublicClientRelayReadback(highLevelForeground);
  const relayReadback = relayReadbackSlice(highLevelForeground);
  const plantedRelayReadback = highLevelForeground.replace(
    relayReadback,
    relayReadback.replace(
      "const rows = await client.db.all(app.todos);",
      "const rows = [{ title: persistedTitleForRun(runNonce) }];",
    ),
  );
  assert.throws(() => assertPublicClientRelayReadback(plantedRelayReadback), /client\.db\.all/);
  for (const decoy of [
    "// const rows = await client.db.all(app.todos);",
    "/* const rows = await client.db.all(app.todos); */",
    'const decoy = "const rows = await client.db.all(app.todos);";',
    "const decoy = `const rows = await client.db.all(app.todos);`;",
    "const decoy = /const rows = await client\\.db\\.all\\(app\\.todos\\);/;",
  ]) {
    const maskedReadback = highLevelForeground.replace(
      relayReadback,
      relayReadback.replace(
        "const rows = await client.db.all(app.todos);",
        `const rows = [{ title: persistedTitleForRun(runNonce) }]; ${decoy}`,
      ),
    );
    assert.throws(
      () => assertPublicClientRelayReadback(maskedReadback),
      /client\.db\.all|try block must contain exactly/,
      `masked relay readback must not accept ${decoy}`,
    );
  }
  for (const mutation of [
    "Object.assign(rows, { map: () => [] });",
    "rows = [];",
    "rows.map = () => [];",
  ]) {
    const mutatedRows = highLevelForeground.replace(
      relayReadback,
      relayReadback.replace(
        "assertPersistedTitleForRun(",
        `${mutation}\n    assertPersistedTitleForRun(`,
      ),
    );
    assert.throws(
      () => assertPublicClientRelayReadback(mutatedRows),
      /try block must contain exactly/,
      `relay readback must reject an intervening rows mutation: ${mutation}`,
    );
  }
  const preTryMutation = highLevelForeground.replace(
    relayReadback,
    relayReadback.replace(
      "try {",
      "Object.assign(client.db, { all: async () => [{ title: persistedTitleForRun(runNonce) }] });\n  try {",
    ),
  );
  assert.throws(
    () => assertPublicClientRelayReadback(preTryMutation),
    /function body must contain exactly/,
    "relay readback must reject a fake public-read mutation before try",
  );
  const postTryMutation = highLevelForeground.replace(
    relayReadback,
    `${relayReadback.slice(0, -1)}\n  void client;\n}`,
  );
  assert.throws(
    () => assertPublicClientRelayReadback(postTryMutation),
    /function body must contain exactly/,
    "relay readback must reject even a post-try no-op",
  );
  const swallowedFailure = highLevelForeground.replace(
    relayReadback,
    relayReadback.replace("} finally {", "} catch { } finally {"),
  );
  assert.throws(
    () => assertPublicClientRelayReadback(swallowedFailure),
    /must not swallow lifecycle failures/,
    "relay readback must reject an empty catch that hides read or assertion failures",
  );
  assertPublicClientSeedStages(highLevelForeground);
  for (const stage of ["open", "subscribe", "write", "read", "publish", "shutdown"]) {
    assert.throws(
      () =>
        assertPublicClientSeedStages(
          highLevelForeground.replace(`markFailure("public-client-${stage}-failed")`, "void 0"),
        ),
      new RegExp(`missing its ${stage} stage|moved away`),
    );
  }
  const misorderedStages = highLevelForeground
    .replace(
      'markFailure("public-client-write-failed")',
      'markFailure("public-client-TEMP-failed")',
    )
    .replace(
      'markFailure("public-client-read-failed")',
      'markFailure("public-client-write-failed")',
    )
    .replace(
      'markFailure("public-client-TEMP-failed")',
      'markFailure("public-client-read-failed")',
    );
  assert.throws(() => assertPublicClientSeedStages(misorderedStages), /stage read is out of order/);
  assert.match(app, /\{\s*contains: \["a"\],\s*excludes: \["b"\],?\s*\}/);
  assert.match(app, /\{\s*contains: \["b"\],\s*excludes: \["a"\],?\s*\}/);
  assert.match(app, /\{\s*write: "a",\s*contains: \["a"\],\s*excludes: \["b"\],?\s*\}/);
  assert.match(app, /\{\s*write: "b",\s*contains: \["b"\],\s*excludes: \["a"\],?\s*\}/);
  assert.match(androidFixture, /@ReactMethod fun acceptancePhase/);
  assert.match(androidFixture, /jazzDeviceAcceptancePhase/);
  assert.match(iosFixture, /RCT_REMAP_METHOD\(acceptancePhase/);
  assert.match(iosFixture, /-JazzDeviceAcceptancePhase/);
});

test("device acceptance is a registered, narrowly scoped pnpm workspace package", () => {
  const workspace = fs.readFileSync(path.resolve(root, "../../pnpm-workspace.yaml"), "utf8");
  const lockfile = fs.readFileSync(path.resolve(root, "../../pnpm-lock.yaml"), "utf8");
  const rootPackage = fs.readFileSync(path.resolve(root, "../../package.json"), "utf8");
  const turbo = fs.readFileSync(path.resolve(root, "../../turbo.json"), "utf8");

  assert.match(workspace, /^  - dev\/rn-device-acceptance$/m);
  assert.match(lockfile, /^  dev\/rn-device-acceptance:/m);
  // Registering the app gives CI its declared Expo dependencies. Explicit
  // package-task overrides prevent the root generic build/test rules from
  // inheriting their `^build` closure; device work remains reachable only
  // through the app's explicit `verify`/`device:*` scripts in its workflow.
  assert.doesNotMatch(rootPackage, /rn-device-acceptance/);
  const turboConfig = JSON.parse(turbo);
  for (const task of ["rn-device-acceptance#test", "rn-device-acceptance#build"]) {
    assert.deepEqual(turboConfig.tasks[task]?.dependsOn, [], `${task} must not inherit ^build`);
  }

  for (const task of ["test", "build"]) {
    // `build` has unrelated repository-wide documentation configuration, so
    // resolve that package's generic build task directly. Root `test` remains
    // intentionally unfiltered: it is the everyday command this registration
    // must keep cheap.
    const args = ["exec", "turbo", "run", task, "--dry-run=json"];
    if (task === "build") args.push("--filter=rn-device-acceptance");
    const graph = JSON.parse(
      execFileSync("pnpm", args, {
        cwd: path.resolve(root, "../.."),
        encoding: "utf8",
      }),
    );
    const acceptanceTask = graph.tasks.find(
      (candidate) => candidate.taskId === `rn-device-acceptance#${task}`,
    );
    assert.ok(
      acceptanceTask,
      `root Turbo graph must resolve the registered acceptance ${task} task`,
    );
    assert.deepEqual(
      acceptanceTask.dependencies,
      [],
      `root Turbo ${task} must not schedule Jazz builds through rn-device-acceptance`,
    );
  }
});

test("Android bootstrap rejects corrupt pinned archives before extraction", () => {
  const bootstrap = read("scripts/bootstrap-android.sh");
  assert.match(bootstrap, /verify-pinned-archive\.sh/);
  assert.match(bootstrap, /refusing corrupt cached Android bootstrap archive/);
  assert.match(bootstrap, /OpenJDK17U-jdk_x64_linux_hotspot_17\.0\.16_8\.tar\.gz/);
  assert.match(bootstrap, /commandlinetools-linux-13114758_latest\.zip/);
  assert.match(bootstrap, /android-ndk-r27b-linux\.zip/);
  assert.match(bootstrap, /33e16af1a6bbabe12cad54b2117085c07eab7e4fa67cdd831805f0e94fd826c1/);
  assert.match(bootstrap, /\.jazz-pinned-sha256/);
  assert.match(bootstrap, /cargo-ndk" --version/);
});

test("dispatch device workflow uses hosted KVM while source jobs remain cheap", () => {
  const workflow = fs.readFileSync(
    path.join(root, "../../.github/workflows/rn-device-acceptance.yml"),
    "utf8",
  );
  assert.match(
    workflow,
    /android-device-acceptance:[\s\S]*runs-on: ubuntu-24\.04[\s\S]*Grant the runner user access to KVM[\s\S]*setfacl -m "u:\$\{USER\}:rw" \/dev\/kvm/,
  );
  assert.match(workflow, /android-source-scaffold:[\s\S]*runs-on: blacksmith-4vcpu-ubuntu-2404/);
  assert.match(workflow, /ios-source-scaffold:[\s\S]*runs-on: blacksmith-6vcpu-macos-15/);
  assert.match(workflow, /\[\[ -r \/dev\/kvm && -w \/dev\/kvm \]\]/);
  assert.match(
    workflow,
    /PATH="\$cache\/cargo\/bin:\$cache\/sdk\/cmdline-tools\/latest\/bin:\$cache\/sdk\/platform-tools:\$cache\/sdk\/emulator:\$PATH"/,
  );
  assert.match(workflow, /scripts\/boot-android-emulator\.sh/);
  assert.doesNotMatch(workflow, /adb wait-for-device/);
  assert.match(workflow, /android-device-acceptance:[\s\S]*timeout-minutes: 45/);
  for (const [job, followingStage] of [
    ["ios-simulator", "Build and stage the relay XCFramework"],
    ["android-device-acceptance", "Grant the runner user access to KVM"],
  ]) {
    const section = jobSection(workflow, job);
    assert.match(
      section,
      new RegExp(
        `pnpm install --frozen-lockfile[\\s\\S]*?pnpm --filter rn-device-acceptance build:workspace-prerequisites[\\s\\S]*?${followingStage}`,
      ),
      `${job} must stage the source-built React Native public entrypoint immediately after install and before native bundling`,
    );
  }
  assertRnDeviceWorkflowContract(workflow);
  assert.throws(
    () =>
      assertRnDeviceWorkflowContract(
        workflow.replace(
          "contains(github.event.pull_request.labels.*.name, 'rn-preview-release')",
          "contains(github.event.pull_request.labels.*.name, 'react-native')",
        ),
      ),
    /identical opt-in contract|same-repository RN-preview PR/,
    "a planted broad Android label must not silently enable a different device gate",
  );
  assert.doesNotMatch(workflow, /labels\.\*\.name, 'react-native'/);
  assert.match(workflow, /scripts\/create-android-avd\.sh[\s\\]+jazz-device-acceptance-api35/);
  assert.doesNotMatch(workflow, /yes no \| avdmanager/);
});

test("Android AVD creation supplies one default-safe answer and fails closed on a second prompt", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-avdmanager-"));
  const transcript = path.join(directory, "transcript");
  const avdHome = path.join(directory, "lane-local-avd");
  const avdmanager = path.join(directory, "avdmanager");
  fs.writeFileSync(
    avdmanager,
    `#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == "create avd --force --name acceptance --package system-images;android-35;google_apis;x86_64" ]]
IFS= read -r first
[[ "$first" == no ]]
if IFS= read -r second; then
  printf 'unexpected extra answer: %s\\n' "$second" >&2
  exit 1
fi
if [[ "\${JAZZ_AVD_REQUIRE_SECOND_PROMPT:-}" == 1 ]]; then
  echo 'second prompt was left unanswered' >&2
  exit 42
fi
if [[ "\${JAZZ_AVD_OMIT_FILES:-}" != 1 ]]; then
  output_home="\${JAZZ_AVD_WRONG_HOME:-$ANDROID_AVD_HOME}"
  mkdir -p "$output_home/acceptance.avd"
  : > "$output_home/acceptance.ini"
  : > "$output_home/acceptance.avd/config.ini"
fi
printf '%s' "$first" > "$JAZZ_AVD_TRANSCRIPT"
`,
    { mode: 0o755 },
  );
  execFileSync(
    "bash",
    [
      path.join(root, "scripts/create-android-avd.sh"),
      "acceptance",
      "system-images;android-35;google_apis;x86_64",
    ],
    {
      env: {
        ...process.env,
        ANDROID_AVD_HOME: avdHome,
        JAZZ_DEVICE_AVDMANAGER: avdmanager,
        JAZZ_AVD_TRANSCRIPT: transcript,
      },
    },
  );
  assert.equal(fs.readFileSync(transcript, "utf8"), "no");
  assert.throws(() =>
    execFileSync(
      "bash",
      [
        path.join(root, "scripts/create-android-avd.sh"),
        "acceptance",
        "system-images;android-35;google_apis;x86_64",
      ],
      {
        env: {
          ...process.env,
          ANDROID_AVD_HOME: avdHome,
          JAZZ_DEVICE_AVDMANAGER: avdmanager,
          JAZZ_AVD_TRANSCRIPT: transcript,
          JAZZ_AVD_REQUIRE_SECOND_PROMPT: "1",
        },
      },
    ),
  );

  const globalAvdHome = path.join(directory, "runner-global-avd");
  assert.throws(
    () =>
      execFileSync(
        "bash",
        [
          path.join(root, "scripts/create-android-avd.sh"),
          "acceptance",
          "system-images;android-35;google_apis;x86_64",
        ],
        {
          env: {
            ...process.env,
            ANDROID_AVD_HOME: path.join(directory, "wrong-placement"),
            JAZZ_DEVICE_AVDMANAGER: avdmanager,
            JAZZ_AVD_TRANSCRIPT: transcript,
            JAZZ_AVD_WRONG_HOME: globalAvdHome,
          },
        },
      ),
    /configured lane-local device/,
    "creation must fail rather than letting boot search a runner-global AVD home",
  );

  assert.throws(
    () =>
      execFileSync(
        "bash",
        [
          path.join(root, "scripts/create-android-avd.sh"),
          "acceptance",
          "system-images;android-35;google_apis;x86_64",
        ],
        {
          env: {
            ...process.env,
            ANDROID_AVD_HOME: path.join(directory, "missing-files"),
            JAZZ_DEVICE_AVDMANAGER: avdmanager,
            JAZZ_AVD_TRANSCRIPT: transcript,
            JAZZ_AVD_OMIT_FILES: "1",
          },
        },
      ),
    /configured lane-local device/,
    "creation must fail when avdmanager reports success without both AVD files",
  );
});

test("checksum pin rejects a planted corrupt cache archive", () => {
  const archive = path.join(
    fs.mkdtempSync(path.join(os.tmpdir(), "jazz-corrupt-cache-")),
    "jdk.tgz",
  );
  fs.writeFileSync(archive, "corrupt");
  assert.throws(() =>
    execFileSync("bash", [
      path.join(root, "scripts/verify-pinned-archive.sh"),
      archive,
      "0".repeat(64),
    ]),
  );
});

test("iOS fixture owns launch-bound metadata and trusted ABI/admission probes", () => {
  const fixture = read("native/ios/JazzDeviceFixture.mm");
  const checkedInFixture = read("ios/JazzDeviceFixture.mm");
  assert.match(fixture, /JazzRelayTrustedAdmission admitScopeJSON/);
  assert.match(fixture, /replaceCapability:self\.capability withScopeJSON/);
  assert.match(fixture, /JazzDeviceScopeFixture\(NSString \*authScope\)/);
  assert.match(fixture, /RCT_REMAP_METHOD\(receiptContext/);
  assert.match(fixture, /@"schema_json": @"\{\\"tables\\":\{\\"todos\\":/);
  assert.match(fixture, /\\"column_type\\":\{\\"type\\":\\"Text\\"\}/);
  assert.doesNotMatch(fixture, /@"schema_json": @"\{\\"tables\\":\{\}\}"/);
  assert.match(fixture, /11111111-1111-4111-8111-111111111111/);
  assert.match(fixture, /fixture-user-a/);
  assert.match(fixture, /fixture-user-b/);
  assert.match(fixture, /22222222-2222-4222-8222-222222222222/);
  assert.match(fixture, /jazz-device-%@\.sqlite/);
  assert.match(fixture, /@"claims": @\{\}/);
  for (const key of ["-JazzDeviceRunNonce", "-JazzDeviceDeviceIdentifier"]) {
    assert.match(fixture, new RegExp(key));
  }
  assert.match(fixture, /#import <CommonCrypto\/CommonDigest\.h>/);
  assert.match(fixture, /NSBundle\.mainBundle\.executablePath/);
  assert.match(fixture, /CC_SHA256\(data\.bytes, \(CC_LONG\)data\.length, digest\)/);
  assert.doesNotMatch(fixture, /JazzDeviceBuildFingerprint/);
  assert.match(fixture, /RCT_REMAP_METHOD\(recordReceipt/);
  for (const candidate of [fixture, checkedInFixture]) {
    assert.match(candidate, /RCT_REMAP_METHOD\(recordDiagnostic/);
    assert.match(candidate, /RCT_REMAP_METHOD\(clearDiagnostic/);
    assert.match(candidate, /jazz-device-diagnostic\.txt/);
    assert.match(candidate, /JazzDeviceDiagnosticCodes\(\) containsObject:detail/);
    assertOrderedDiagnosticCodes(candidate, "ios");
    assert.throws(
      () => assertOrderedDiagnosticCodes(swapSubscriptionWriteStages(candidate), "ios"),
      /must exactly follow the shared order/,
      "swapping two codes must fail even when generated and checked-in fixtures agree",
    );
  }
  assert.match(fixture, /JAZZ_DEVICE_RESULT/);
  assert.match(fixture, /NSDataWritingAtomic/);
  assert.doesNotMatch(fixture, /recordReceipt[\s\S]*JazzRelayTrustedAdmission/);
});

test("iOS acceptance embeds JavaScript and reports launch diagnostics on receipt timeout", () => {
  const workflow = fs.readFileSync(
    path.resolve(root, "../../.github/workflows/rn-device-acceptance.yml"),
    "utf8",
  );
  const driver = read("scripts/run-ios.mjs");
  assert.match(workflow, /-configuration Release -sdk iphonesimulator/);
  assert.match(workflow, /Release-iphonesimulator\/JazzRNdeviceacceptance\.app/);
  for (const detail of [
    "parseLaunchProcessId",
    "get_app_container",
    "jazz-device-receipt.ndjson",
    "jazz-device-diagnostic.txt",
    "app JavaScript/native diagnostic",
    "receiptFile",
    "rmSync\\(receiptFilePath\\(\\)",
    "launchctl",
    "recent app logs \\(capped\\)",
    'process == "JazzRNdeviceacceptance"',
  ]) {
    assert.match(driver, new RegExp(detail));
  }
  assert.match(driver, /assertDeviceReceipt\(receiptFile\(\), expected\)/);
  // The sandbox file is the receipt transport. App logs remain diagnostics
  // only: release React Native logging is not reliable evidence transport.
  assert.doesNotMatch(driver, /assertDeviceReceipt\(receiptOutput\(\), expected\)/);
  assert.doesNotMatch(driver, /eventMessage CONTAINS 'JAZZ_DEVICE_RESULT'/);
  const app = read("App.tsx");
  assert.match(app, /await proveAdmittedRelay/);
  assert.match(app, /installNativeForegroundRuntime/);
  assert.match(app, /proveForegroundByteAbi/);
  assert.match(app, /proveForegroundRevoked/);
  assert.match(app, /encodeNativeForegroundCommand/);
  assert.match(app, /decodeNativeForegroundResponse/);
  assert.match(app, /await proveLogoutRevocation/);
  assert.match(app, /await proveAuthScopeSwitch/);
  assert.match(app, /switchNativeRelayAuthScope/);
  assert.match(app, /logoutNativeRelay/);
  assert.match(app, /oldScopeForeground = foregroundFactory\.openAttached\(scopeA\.capability\)/);
  assert.match(
    app,
    /markFailure\("foreground-open-failed"\);\s+const revocableForeground = foregroundFactory\.openAttached\(capability\)/,
  );
  assert.match(app, /proveForegroundRevoked\(oldScopeForeground, foregroundCodec\.encode\)/);
  assert.match(
    app,
    /proveForegroundByteAbi\(foregroundFactory, scopeB\.capability, foregroundCodec, markFailure\)/,
  );
  assert.match(app, /await recordDeviceReceipt\(results\.join\("\\n"\)\)/);
  assert.match(app, /createDeviceDiagnosticTracker/);
  assert.match(app, /observeTrustedAdmissionLifecycle\(diagnostic\.mark\)/);
  assert.match(app, /await diagnostic\.clear\(\)/);
  assert.match(app, /diagnostic\.mark\("receipt-write-failed"\)/);
  assert.match(app, /diagnostic\.retry\(\)/);
  assert.doesNotMatch(app, /reason instanceof Error|String\(reason\)|\.message/);
  assert.ok(
    app.indexOf("await observeTrustedAdmissionLifecycle(diagnostic.mark)") <
      app.indexOf("await diagnostic.clear()") &&
      app.indexOf("await diagnostic.clear()") < app.indexOf("await recordDeviceReceipt"),
    "the native receipt sink must run only after the complete JS relay lifecycle proof",
  );
});
