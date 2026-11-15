import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

// App-file examples (jazz.app.json, jazz.permissions.json) stay readable:
// each table, column map, relation map and definition map is spread over lines,
// never `"columns": { "title": "string", … }` on one line.
const root = path.join(__dirname, "../content");
const files: string[] = [];
const walk = (dir: string) => {
  for (const name of readdirSync(dir)) {
    const p = path.join(dir, name);
    if (statSync(p).isDirectory()) walk(p);
    else if (p.endsWith(".mdx")) files.push(p);
  }
};
walk(root);

const INLINE = /"(tables|columns|relations|defs)": \{ "/;
const INLINE_TABLE = /^\s*"[\w$]+": \{ "(columns|read|insert|update|delete)"/;

describe("app-file examples", () => {
  it.each(files.map((f) => [path.relative(root, f), f]))(
    "%s spreads its maps over lines",
    (_, f) => {
      const blocks = readFileSync(f, "utf8").match(/```json[^\n]*\n[\s\S]*?```/g) ?? [];
      for (const block of blocks) {
        if (!/"(tables|defs)"/.test(block)) continue;
        for (const line of block.split("\n")) {
          expect(INLINE.test(line) || INLINE_TABLE.test(line), line.trim()).toBe(false);
        }
      }
    },
  );
});
