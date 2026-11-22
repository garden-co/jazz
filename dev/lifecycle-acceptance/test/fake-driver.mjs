// A driver that lies, to test the suite. FAKE=yes: everything works. FAKE=throws: the second open
// throws something that isn't StoreHeld.
import { createInterface } from "node:readline";

const mode = process.env.FAKE;
const opened = new Set();
console.log(JSON.stringify({ ready: true }));
createInterface({ input: process.stdin }).on("line", (line) => {
  const { id, op, dir } = JSON.parse(line);
  let reply = { id, ok: true, v: op === "read" ? [] : "x" };
  if (op === "open") {
    if (mode === "throws" && opened.has(dir))
      reply = { id, ok: false, kind: "io", msg: "Storage: database is locked" };
    opened.add(dir);
  }
  console.log(JSON.stringify(reply));
});
