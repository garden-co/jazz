// Typecheck every ```ts title="schema.ts"``` block of the docs against jazz-tools' DSL, and print
// the permissions each one compiles to next to the page's "Generated JSON" tab.
//   node --experimental-strip-types docs/scripts/check-schema-ts-blocks.mts [--json]
// A block with only `definePermissions` is checked with the page's latest `s.defineSchema` block;
// `// ...` inside `definePermissions` stands for the page's other tables (filled in with `{}`).
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";

const repo = new URL("../../", import.meta.url).pathname;
const content = join(repo, "docs/content");
const out = join(repo, "packages/jazz-tools/.doc-snippets");
rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });

const TOOLS =
  "definePermissions, exists, allowedTo, session, anyOf, allOf, not, ne, gt, gte, lt, lte, contains, eq, isIn";
const files: string[] = [];
(function walk(d: string) {
  for (const f of readdirSync(d)) {
    const p = join(d, f);
    if (statSync(p).isDirectory()) walk(p);
    else if (f.endsWith(".mdx")) files.push(p);
  }
})(content);

interface Snippet { file: string; line: number; name: string; json?: string }
const snippets: Snippet[] = [];
for (const file of files) {
  const text = readFileSync(file, "utf8");
  const re = /^([ \t]*)```(ts|json)([^\n]*)\n([\s\S]*?)^\1```/gm;
  let m: RegExpExecArray | null;
  const schemaBlocks: string[] = [];
  let last: Snippet | undefined;
  while ((m = re.exec(text))) {
    const [, , lang, meta, body] = m;
    const line = text.slice(0, m.index).split("\n").length;
    if (lang === "json") {
      if (last && /jazz\.permissions\.json/.test(meta) && line - last.line < 60) last.json = body;
      continue;
    }
    if (!/title="(schema|permissions)\.ts"/.test(meta) || /nocheck/.test(meta)) continue;
    let code = body;
    const hasSchema = /s\.defineSchema\(/.test(code);
    // The schema a permissions-only block goes with: the latest one with every table it lists.
    const listed = [...code.slice(code.indexOf("definePermissions(")).matchAll(/^ {2}(\w+): /gm)].map((x) => x[1]);
    const tablesOf = (b: string) => [...b.matchAll(/^ {2}(\w+): s\b/gm)].map((x) => x[1]);
    const schemaBlock = [...schemaBlocks].reverse().find((b) => listed.every((t) => tablesOf(b).includes(t)));
    if (hasSchema && !/definePermissions\(/.test(code) && !/claims:/.test(code))
      schemaBlocks.push(code.replace(/\n(const \{[^}]*\} =\s*schema;|export const permissions)[\s\S]*$/, "\n"));
    else if (!hasSchema && schemaBlock) code = `${schemaBlock}\n${code}`;
    else if (!hasSchema) continue;
    if (/from "\.\//.test(code)) continue; // imports another file of the app
    if (!/= ?\n?\s*schema;/.test(code) && /definePermissions\(/.test(code))
      code = code.replace(/(\n(const |export const permissions))/, `\nconst { ${TOOLS} } = schema;\n$1`);
    if (!/from "jazz-tools"/.test(code)) code = `import { schema as s } from "jazz-tools";\n${code}`;
    // Fill `// ...` with the tables the block leaves out.
    if (/^\s*\/\/ \.\.\.\s*$/m.test(code)) {
      const tables = [...(schemaBlock ?? "").matchAll(/^ {2}(\w+): s\b/gm)].map((x) => x[1]);
      const perm = code.slice(code.indexOf("definePermissions({"));
      const listed = new Set([...perm.matchAll(/^ {2}(\w+): /gm)].map((x) => x[1]));
      const fill = tables.filter((t) => !listed.has(t)).map((t) => `  ${t}: {},`).join("\n");
      code = code.replace(/^\s*\/\/ \.\.\.\s*$/m, fill);
    }
    code = code.replace(/from "jazz-tools"/g, 'from "../src/authoring.js"');
    if (!/export const permissions/.test(code)) code += "\nconst permissions = undefined;\n";
    code += `\nimport { readablePermissions as __rp } from "../src/permissions/dsl.js";\nexport const __readable = permissions === undefined ? undefined : __rp(permissions);\n`;
    const name = `${relative(content, file).replace(/[/.]/g, "_")}_${line}.ts`;
    writeFileSync(join(out, name), code);
    last = { file: relative(repo, file), line, name };
    snippets.push(last);
  }
}
writeFileSync(
  join(out, "tsconfig.json"),
  JSON.stringify({ extends: "../tsconfig.json", compilerOptions: { noEmit: true, noUnusedLocals: false, rootDir: ".." }, include: ["./*.ts"] }),
);
let tscOut = "";
try {
  execFileSync("npx", ["tsc", "-p", join(out, "tsconfig.json")], { cwd: join(repo, "packages/jazz-tools"), stdio: "pipe" });
} catch (e: any) {
  tscOut = String(e.stdout);
}
const errors = new Map<string, string[]>();
for (const l of tscOut.split("\n")) {
  const mm = /\.doc-snippets\/([^(]+)\((\d+)/.exec(l);
  if (mm) errors.set(mm[1], [...(errors.get(mm[1]) ?? []), l.replace(/^.*?\.doc-snippets\//, "")]);
}
for (const s of snippets) {
  const errs = errors.get(s.name) ?? [];
  console.log(`${errs.length ? "FAIL" : "ok  "} ${s.file}:${s.line}`);
  for (const e of errs) console.log(`     ${e}`);
}
if (process.argv.includes("--json")) {
  for (const s of snippets) {
    let readable: unknown;
    try {
      const mod = await import(join(out, s.name));
      readable = mod.__readable;
    } catch (e: any) {
      readable = `ERROR ${e.message}`;
    }
    console.log(`\n=== ${s.file}:${s.line}\n--- compiled:\n${JSON.stringify(readable, null, 2)}${s.json ? `\n--- page:\n${s.json}` : ""}`);
  }
}
process.exit(errors.size ? 1 : 0);
