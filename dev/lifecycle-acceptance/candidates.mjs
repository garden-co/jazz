// The candidates of this run: the dispatch's, else candidates.json's.
import { readFileSync } from "node:fs";
import { checkCandidate } from "./lib/provenance.mjs";

const { SHA, RUN } = process.env;
const list =
  SHA || RUN
    ? [{ name: (SHA ?? "").slice(0, 8), sha: SHA, run: RUN }]
    : JSON.parse(readFileSync(new URL("candidates.json", import.meta.url), "utf8"));
if (!Array.isArray(list) || list.length === 0) throw new Error("no candidates");
for (const candidate of list) {
  checkCandidate(candidate);
  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(candidate.name ?? "")) throw new Error(`bad candidate name '${candidate.name}'`);
}
if (new Set(list.map((c) => c.name)).size !== list.length) throw new Error("candidate names repeat");
console.log(`list=${JSON.stringify(list)}`);
