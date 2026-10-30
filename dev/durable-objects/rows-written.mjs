// Usage: node rows-written.mjs TRACE.jsonl
// TRACE comes from a binary run with JAZZ_SQL_TRACE and
// JAZZ_SQL_TRACE_EXPANDED=1 (jazz's sql-trace feature), with
// {"mark": phase} lines between phases. Replays the first connection's
// statements (the authority's) on Durable Object SQLite and prints billed
// rows written and read per phase. Transaction statements and PRAGMAs are skipped:
// DO refuses them, and their scopes do not change what a commit writes.
import fs from "node:fs";
import { Miniflare } from "miniflare";

const lines = fs
  .readFileSync(process.argv[2], "utf8")
  .split("\n")
  .filter(Boolean)
  .map((l) => JSON.parse(l));
const conn = lines.find((l) => l.conn)?.conn;
const skip = /^\s*(SAVEPOINT|RELEASE|ROLLBACK|BEGIN|COMMIT|PRAGMA)\b/i;
let rollbacks = 0;

// Long blob literals become bound parameters: DO caps statement text at
// 100 KB, and bound values only at 2 MiB.
function parameterize(text) {
  const blobs = [];
  let out = "";
  for (let i = 0; i < text.length; ) {
    const c = text[i];
    if ((c === "x" || c === "X") && text[i + 1] === "'" && !/[\w]/.test(text[i - 1] ?? "")) {
      const end = text.indexOf("'", i + 2);
      const hex = text.slice(i + 2, end);
      if (hex.length > 2000) {
        blobs.push(hex);
        out += "?";
      } else out += text.slice(i, end + 1);
      i = end + 1;
    } else if (c === "'") {
      let j = i + 1;
      while (j < text.length && !(text[j] === "'" && text[j + 1] !== "'"))
        j += text[j] === "'" ? 2 : 1;
      out += text.slice(i, j + 1);
      i = j + 1;
    } else if (c === '"') {
      const end = text.indexOf('"', i + 1);
      out += text.slice(i, end + 1);
      i = end + 1;
    } else {
      out += c;
      i += 1;
    }
  }
  return { sql: out, blobs };
}

const ops = [];
for (const line of lines) {
  if (line.mark) ops.push({ mark: line.mark });
  else if (line.conn === conn) {
    if (/^\s*ROLLBACK/i.test(line.sql)) rollbacks += 1;
    if (skip.test(line.sql) || !line.expanded) continue;
    ops.push(parameterize(line.expanded));
  }
}
const mf = new Miniflare({
  modules: true,
  scriptPath: new URL("./rows-written.js", import.meta.url).pathname,
  compatibilityDate: "2026-07-01",
  durableObjects: { ROWS: { className: "Rows", useSQLite: true } },
});
const phases = await (
  await mf.dispatchFetch("http://rows/", { method: "POST", body: JSON.stringify({ ops }) })
).json();
console.log(JSON.stringify({ connection: conn, rollbacks, phases }, null, 2));
await mf.dispose();
