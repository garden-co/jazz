// index.mjs lists its ESM named exports by hand (see its header), so a
// function added to the binding must be added there too: every function
// index.d.ts declares is exported, except the ones consumers load through
// CommonJS on purpose.
const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const { test } = require("node:test");

// `serveCore` is required from a separate process through CommonJS
// (jazz-tools' Vercel store), never imported as ESM.
const COMMONJS_ONLY = new Set(["serveCore"]);

test("index.mjs exports every function index.d.ts declares", () => {
  const declarations = readFileSync(join(__dirname, "../index.d.ts"), "utf8");
  const module = readFileSync(join(__dirname, "../index.mjs"), "utf8");
  const exported = new Set(
    module
      .slice(module.indexOf("export const {"), module.indexOf("} = nativeBinding"))
      .match(/[A-Za-z_$][\w$]*/g),
  );
  const declared = [...declarations.matchAll(/^export declare function (\w+)/gm)].map((m) => m[1]);
  assert.ok(declared.length > 20, `found ${declared.length} declarations`);
  const missing = declared.filter((name) => !exported.has(name) && !COMMONJS_ONLY.has(name));
  assert.deepEqual(missing, [], "add these to index.mjs's export list");
});
