#!/usr/bin/env node
// Apply #jazz-thin-startup's fix 1 (token mint: 4-bit Ed25519 window + token
// reuse; copied verbatim from results/2026-11-14/thin-startup/patch/fix-mint.mjs
// on branch profile/thin-startup) to the installed dist in place, to measure
// it together with the local-first prototypes.
//   node with-thin-fix-mint.mjs <app dir> [--restore]
import { copyFileSync, existsSync, readFileSync, writeFileSync, realpathSync } from "node:fs";
import { join } from "node:path";
import { apply } from "./thin-fix-mint.mjs";

const dist = join(realpathSync(join(process.argv[2], "node_modules/jazz-tools")), "dist");
const file = join(dist, "thin/self-signed-token.js");
const base = `${file}.pre-mint`;
if (process.argv.includes("--restore")) {
  if (existsSync(base)) copyFileSync(base, file);
  process.exit(0);
}
if (!existsSync(base)) copyFileSync(file, base);
let src = readFileSync(base, "utf8");
const edit = (_rel, find, replace) => {
  const n = src.split(find).length - 1;
  if (n !== 1) throw new Error(`expected 1 match, found ${n}: ${find.slice(0, 80)}`);
  src = src.replace(find, () => replace);
};
// The thin thread's marks wrapper, which fix-mint builds on.
edit("", "export function mintSelfSignedToken(issuer, options) {",
  "export function mintSelfSignedToken(issuer, options) {\n    return mintSelfSignedTokenInner(issuer, options);\n}\nfunction mintSelfSignedTokenInner(issuer, options) {");
apply({ edit });
writeFileSync(file, src);
console.error("patched thin/self-signed-token.js (thin fix-mint)");
