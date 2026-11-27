#!/usr/bin/env node
// Make patched copies of the installed jazz-tools (2.0.0-alpha.62, dist/) next
// to it in the pnpm store, for thin-reload.mjs --jazz-tools <name>.
//
//   node patch/apply.mjs marks      -> jazz-tools-marks:     performance marks only
//   node patch/apply.mjs fix-mint   -> jazz-tools-fix-mint:  marks + token cache (fix 1)
//   node patch/apply.mjs fix-socket -> jazz-tools-fix-socket: marks + socket without the IndexedDB lookup (fix 2)
//   node patch/apply.mjs fix-pipe   -> jazz-tools-fix-pipe:  marks + pipelined schema/subscribe (fix 3)
//   node patch/apply.mjs fix-all    -> all of the above
//
// Every edit is a literal search/replace that must match exactly once, so a
// different jazz-tools version fails loudly instead of measuring nothing.
import { cpSync, existsSync, readFileSync, readlinkSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const appDir = resolve(here, "../../../../stage-plan/jazz");
const link = join(appDir, "node_modules/jazz-tools");
const installed = resolve(dirname(link), readlinkSync(link));
const which = process.argv[2] ?? "marks";
const fixes = which === "fix-all" ? ["fix-mint", "fix-socket", "fix-pipe"] : which === "marks" ? [] : [which];
const target = join(dirname(installed), `jazz-tools-${which}`);
if (existsSync(target)) rmSync(target, { recursive: true });
cpSync(installed, target, { recursive: true, dereference: false });
const dist = join(target, "dist");

const M = (name, detail = "undefined") => `globalThis.performance?.mark?.(${JSON.stringify(name)}, { detail: ${detail} });`;
const edit = (file, find, replace) => {
  const path = join(dist, file);
  const src = readFileSync(path, "utf8");
  const n = src.split(find).length - 1;
  if (n !== 1) throw new Error(`${file}: expected 1 match, found ${n}:\n${find}`);
  writeFileSync(path, src.replace(find, replace));
};

// ------------------------------------------------------------------ marks
// Module evaluation: react/index.js runs after everything it imports.
edit("react/index.js", `export { JazzProvider`, `${M("jz:eval:react-index")}\nexport { JazzProvider`);
edit("thin/self-signed-token.js", `export function mintSelfSignedToken(issuer, options) {`,
  `${M("jz:eval:self-signed-token")}\nexport function mintSelfSignedToken(issuer, options) {\n    ${M("jz:mint:start")}\n    try { return mintSelfSignedTokenInner(issuer, options); } finally { ${M("jz:mint:end")} }\n}\nfunction mintSelfSignedTokenInner(issuer, options) {`);
// Session core: accounts (restore + token), session owner, first backend answer.
edit("session/create-jazz-session.js", `export async function openBrowserSessionCore(config) {`,
  `export async function openBrowserSessionCore(config) {\n    ${M("jz:session:start")}`);
edit("session/create-jazz-session.js", `    const accounts = await createAccountManagerWithMinter({ ...config, serverUrl }, mintLocalFirstTokenInJs, `,
  `    ${M("jz:accounts:start")}\n    const accounts = await createAccountManagerWithMinter({ ...config, serverUrl }, mintLocalFirstTokenInJs, `);
edit("session/create-jazz-session.js", `    session = await createJazzSessionOwner({`,
  `    ${M("jz:accounts:end")}\n    ${M("jz:session-owner:start")}\n    session = await createJazzSessionOwner({`);
edit("session/create-jazz-session.js", `    await firstBackendAnswer(session);\n    return session;`,
  `    ${M("jz:session-owner:end")}\n    await firstBackendAnswer(session);\n    ${M("jz:session:ready")}\n    return session;`);
edit("session/create-jazz-session.js", `    const openThin = async (account) => {`,
  `    const openThin = async (account) => {\n        ${M("jz:open-thin:start")}`);
// Runtime client creation (lazy: the first query) and connecting.
edit("runtime/connection-manager/direct-connection-manager.js", `    onClientCreated({ client }) {`,
  `    onClientCreated({ client }) {\n        ${M("jz:client-created")}`);
edit("runtime/connection-manager/direct-connection-manager.js", `        client.connectTransport(config.serverUrl, auth);`,
  `        ${M("jz:connect-transport")}\n        client.connectTransport(config.serverUrl, auth);`);
// Thin runtime: socket, negotiation, schema bind, first subscribe, first delta.
edit("thin/thin-runtime.js", `        const carrier = new WebSocketCarrier({`,
  `        ${M("jz:thin:open")}\n        const carrier = new WebSocketCarrier({`);
edit("thin/thin-runtime.js", `        void carrier.ready().then((negotiation) => {`,
  `        void carrier.ready().then((negotiation) => {\n            ${M("jz:thin:negotiated")}`);
edit("thin/thin-runtime.js", `                this.attempt = 0;\n                this.bound = true;`,
  `                ${M("jz:thin:schema-bound")}\n                this.attempt = 0;\n                this.bound = true;`);
edit("thin/thin-runtime.js", `        this.sendFrame(carrier, encodeThinRequest(id, { type: "schema", schema: this.schemaBytes }), (bytes) => {`,
  `        ${M("jz:thin:schema-sent", "{ bytes: this.schemaBytes.byteLength }")}\n        this.sendFrame(carrier, encodeThinRequest(id, { type: "schema", schema: this.schemaBytes }), (bytes) => {`);
edit("thin/thin-runtime.js", `        live.requestId = id;\n        this.liveByRequest.set(id, handle);`,
  `        live.requestId = id;\n        globalThis.__jzSubs = (globalThis.__jzSubs ?? 0) + 1;\n        ${M("jz:thin:subscribe-sent", "{ n: globalThis.__jzSubs }")}\n        this.liveByRequest.set(id, handle);`);
edit("thin/thin-runtime.js", `            live.delivered = true;`,
  `            live.delivered = true;\n            globalThis.__jzDeltas = (globalThis.__jzDeltas ?? 0) + 1;\n            ${M("jz:thin:delta", "{ n: globalThis.__jzDeltas }")}`);

// ------------------------------------------------------------------ fixes
for (const fix of fixes) (await import(`./${fix}.mjs`)).apply({ edit, M, dist });
console.log(`${target} (${["marks", ...fixes].join(" + ")})`);
