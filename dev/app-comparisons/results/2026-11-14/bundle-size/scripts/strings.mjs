// Prose string bytes per package in a minified chunk: parse with acorn, map each
// string/template literal back through the sourcemap to its package.
import fs from "fs"; import zlib from "zlib";
import * as acorn from "acorn"; import * as walk from "acorn-walk"; import { SourceMapConsumer } from "source-map";
const [file] = process.argv.slice(2);
const code = fs.readFileSync(file, "utf8"); const map = JSON.parse(fs.readFileSync(file + ".map", "utf8"));
const ast = acorn.parse(code, { ecmaVersion: "latest", sourceType: "module", locations: true });
const smc = await new SourceMapConsumer(map); const acc = {};
const add = (node, text) => {
  if (text.length < 40 || !/[a-z]{2,} [a-z]{2,} [a-z]{2,}/i.test(text)) return;
  const pos = smc.originalPositionFor({ line: node.loc.start.line, column: node.loc.start.column });
  const pkg = (pos.source ?? "?").match(/node_modules\/(?:\.pnpm\/[^/]+\/node_modules\/)?((?:@[^/]+\/)?[^/]+)/)?.[1] ?? "app";
  (acc[pkg] ??= []).push(text);
};
walk.full(ast, (n) => {
  if (n.type === "Literal" && typeof n.value === "string") add(n, n.value);
  if (n.type === "TemplateLiteral") add(n, n.quasis.map((q) => q.value.cooked ?? "").join(" "));
});
for (const [p, list] of Object.entries(acc).sort((a, b) => b[1].join("").length - a[1].join("").length).slice(0, 6)) {
  const j = list.join("\n");
  console.log(`${p}: ${list.length} prose strings, ${(j.length / 1024).toFixed(1)} KB raw, ~${(zlib.gzipSync(j).length / 1024).toFixed(1)} KB gzip`);
}
