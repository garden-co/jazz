// usage: node check-newer.mjs <extracted jazz-tools package dir>
import { pathToFileURL } from "node:url";
import { join } from "node:path";
const dir = process.argv[2];
const m = await import(pathToFileURL(join(dir, "dist/backend/vercel-store.js")).href).catch(
  (e) => ({ err: e }),
);
if (!m.newerVersion) {
  console.log("newerVersion not exported:", m.err?.message ?? Object.keys(m).slice(0, 20));
  process.exit(2);
}
const cases = [
  ["2.0.0-alpha.65", "2.0.0-alpha.65-rc.1", true],
  ["2.0.0-alpha.64", "2.0.0-alpha.64-rc.1", true],
  ["2.0.0-alpha.65-rc.1", "2.0.0-alpha.64-rc.1", true],
  ["2.0.0-alpha.65-rc.1", "2.0.0-alpha.64", true],
  ["2.0.0-alpha.65-rc.2", "2.0.0-alpha.65-rc.1", true],
  ["2.0.0-alpha.100-rc.1", "2.0.0-alpha.99-rc.1", true],
  ["2.0.0-alpha.65-rc.1", "2.0.0-alpha.65", false],
  ["2.0.0-alpha.64-rc.1", "2.0.0-alpha.64", false],
];
let bad = 0;
for (const [a, b, want] of cases) {
  const got = m.newerVersion(a, b);
  if (got !== want) bad++;
  console.log(`${got === want ? "ok " : "BAD"} newerVersion(${a}, ${b}) = ${got} (want ${want})`);
}
process.exit(bad ? 1 : 0);
