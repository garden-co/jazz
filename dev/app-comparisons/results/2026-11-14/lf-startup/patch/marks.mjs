#!/usr/bin/env node
// Measurement-only patch: performance.mark()s inside the published
// jazz-tools@2.0.0-alpha.62 dist (page modules and the bundled SharedWorker).
// All marks are named `lf:*`; harness/breakdown.mjs collects them from the
// page and the worker like any other mark.
//
//   node marks.mjs <app dir> [--restore]
//
// Keeps a .orig copy of each file it touches; --restore puts them back.
import { copyFileSync, existsSync, readFileSync, writeFileSync, realpathSync } from "node:fs";
import { join } from "node:path";

const app = process.argv[2];
const restore = process.argv.includes("--restore");
const dist = join(realpathSync(join(app, "node_modules/jazz-tools")), "dist");
const M = (name) => `globalThis.performance?.mark?.(${JSON.stringify(name)});`;

/** [file, [[anchor, replacement-with-$&], ...]] */
const edits = [
  ["session/create-jazz-session.js", [
    ["    refuseLocalFirstTiming(localFirst, loadLocalFirst);", `$&\n    ${M("lf:session:accounts:start")}`],
    ["    session = await createJazzSessionOwner({", `    ${M("lf:session:accounts:ready")}\n$&`],
    ["    const openClient = async (account) => {", `$&\n        ${M("lf:session:openClient")}`],
    ["                module = await loadLocalFirst();", `                ${M("lf:chunk:import:start")}\n$&\n                ${M("lf:chunk:import:end")}`],
  ]],
  ["accounts/context.js", [
    ["        lease = startBrowserWorkerLease({", `        ${M("lf:createDb:start")}\n$&`],
    ["        const resolved = await resolveAccountRuntimeConfig(config);", `$&\n        ${M("lf:auth:resolved")}`],
    ["        db = await createDbWithRuntimeSource(resolved, runtimeSource, opening);", `$&\n        ${M("lf:createDb:end")}`],
  ]],
  ["runtime/db.js", [
    ["    await runtimeSource.load(config);", `    ${M("lf:load:start")}\n$&\n    ${M("lf:load:end")}`],
    ["    const db = browserWorker\n        ? await Db.createWithBrowserWorker(", `    ${M("lf:db:create")}\n$&`],
  ]],
  ["runtime/connection-manager/browser-connection-manager.js", [
    ["        this.foregroundNodeLease = await (this.earlyLease ??", `        ${M("lf:lease:await")}\n$&`],
    ["            this.host.runtimeSource.acquireBrowserForegroundNodeLease(this.host.config));\n    }", `            this.host.runtimeSource.acquireBrowserForegroundNodeLease(this.host.config));\n        ${M("lf:lease:got")}\n    }`],
    ["        this.browserConnectionInput = input;", `        ${M("lf:client-created")}\n$&`],
    ["        const readiness = connection.ready();", `$&\n        readiness.then(() => ${M("lf:worker-connection:ready").slice(0, -1)}, () => {});`],
  ]],
  ["runtime/wasm-loader.js", [
    ['    const wasmModule = await import("jazz-wasm");', `    ${M("lf:wasm:import:start")}\n$&\n    ${M("lf:wasm:import:end")}`],
    ["    const response = await fetch(wasmUrl);", `    ${M("lf:wasm:fetch:start")}\n$&\n    ${M("lf:wasm:fetch:headers")}`],
    ["            compiled = await WebAssembly.compileStreaming(response);", `$&\n            ${M("lf:wasm:compiled")}`],
    ["        await wasmModule.default({ module_or_path: compiled });", `$&\n        ${M("lf:wasm:instantiated")}`],
  ]],
  ["runtime/default-runtime-source.js", [
    ["        const mainThreadPeerRuntime = this.nativeSchemaView(", `        ${M("lf:page-runtime:new:start")}\n$&`],
    ["        return JazzClient.connectWithRuntime(this.wrapClientRuntime(mainThreadPeerRuntime, config, schema), context, runtimeOptions);", `        ${M("lf:page-runtime:new:end")}\n$&`],
  ]],
  ["worker/jazz-broker-worker.js", [
    // wasm-loader (bundled copy)
    ["  const response = await fetch(wasmUrl);", `  ${M("lf:wasm:fetch:start")}\n$&\n  ${M("lf:wasm:fetch:headers")}`],
    ["      compiled2 = await WebAssembly.compileStreaming(response);", `$&\n      ${M("lf:wasm:compiled")}`],
    ["    await wasmModule2.default({ module_or_path: compiled2 });", `$&\n    ${M("lf:wasm:instantiated")}`],
    // store open
    ["  const epoch = await acquireBrowserPhysicalDatabaseEpoch(dbName);", `  ${M("lf:store:epoch:start")}\n$&\n  ${M("lf:store:epoch:end")}`],
    ["    pageStore = await IndexedDbPageStore.open(dbName, { owner: storageOwner });\n    await pageStore.claimBrowserWorkerEpoch(epoch.id, epoch);",
      `    pageStore = await IndexedDbPageStore.open(dbName, { owner: storageOwner });\n    ${M("lf:store:idb-open:end")}\n    await pageStore.claimBrowserWorkerEpoch(epoch.id, epoch);\n    ${M("lf:store:epoch-claimed")}`],
    ["    const physicalOwner = await ensurePhysicalDatabaseOwner(options.dbName, options.storageOwner);",
      `    ${M("lf:init:start")}\n$&\n    ${M("lf:init:owner")}`],
    ["    const wasmModule2 = await loadWorkerWasmModule(options.runtimeSources);", `$&\n    ${M("lf:init:wasm")}`],
    ["    const node = context2.pageStore.replicaNode;", `    ${M("lf:init:openBrowser:start")}\n$&`],
    ["    const runtime = NativeRuntimeAdapter.fromDb(", `    ${M("lf:init:openBrowser:end")}\n$&`],
    ["    await context2.initialize;\n    assertAdmission();", `    ${M("lf:connect:await-init")}\n    await context2.initialize;\n    ${M("lf:connect:initialized")}\n    assertAdmission();`],
    ['    post(port, { type: "runtime-ready" });', `    ${M("lf:connect:runtime-ready")}\n$&`],
  ]],
];

for (const [rel, list] of edits) {
  const file = join(dist, rel);
  const orig = `${file}.orig`;
  if (restore) {
    if (existsSync(orig)) copyFileSync(orig, file);
    continue;
  }
  if (!existsSync(orig)) copyFileSync(file, orig);
  let src = readFileSync(orig, "utf8");
  for (const [anchor, repl] of list) {
    const n = src.split(anchor).length - 1;
    if (n !== 1) throw new Error(`${rel}: anchor found ${n}x: ${anchor.slice(0, 80)}`);
    src = src.replace(anchor, () => repl.replace("$&", anchor));
  }
  writeFileSync(file, src);
  console.error(`patched ${rel} (${list.length})`);
}
