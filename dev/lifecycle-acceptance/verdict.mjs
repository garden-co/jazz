// PASS / FAIL / NOT_COVERED per rule and language, from suite/run.mjs's results.
// usage: CANDIDATES='<json list>' node verdict.mjs <results dir>
// Fails closed: a language without results, or a rule without a result, is a FAIL.
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { LANGS } from "./lib/common.mjs";
import { RULES } from "./suite/rules/index.mjs";

const dir = process.argv[2];
const candidates = JSON.parse(process.env.CANDIDATES ?? "[]");
if (!dir || !candidates.length) {
  console.log("FAIL verdict: no results dir or no candidates");
  process.exit(1);
}

const summary = [];
let green = true;
for (const { name, sha, run, mustFail = [] } of candidates) {
  const byLang = {};
  for (const lang of LANGS) {
    const file = join(dir, `${name}-${lang}.json`);
    if (!existsSync(file)) continue;
    const result = JSON.parse(readFileSync(file, "utf8"));
    if (result.sha === sha && result.run === String(run) && result.lang === lang) byLang[lang] = result;
  }
  const totals = { PASS: 0, FAIL: 0, NOT_COVERED: 0 };
  const rows = [];
  const reasons = [];
  const status = {};
  for (const rule of RULES) {
    const cells = LANGS.map((lang) => {
      const got = byLang[lang]?.results.find((result) => result.id === rule.id);
      const is = got?.status ?? "FAIL";
      totals[is]++;
      status[`${rule.id} ${lang}`] = is;
      if (!got) {
        console.log(`FAIL ${name} ${lang} ${rule.id}: no result (setup broke, see the job)`);
        return "FAIL (no result)";
      }
      const count = is === "NOT_COVERED" ? "" : ` ${got.passed}/${got.rounds}`;
      const worst = Object.entries(got.failures).sort((a, b) => b[1].count - a[1].count)[0];
      const why = is === "FAIL" && worst ? `${worst[1].count}x ${worst[0]}` : got.why;
      console.log(`${is} ${name} ${lang} ${rule.id}${count}${why ? `: ${why}` : ""}`);
      if (why && is !== "PASS") reasons.push(`- \`${rule.id}\` ${lang} ${is}: ${why.replace(/\s+/g, " ").slice(0, 300)}`);
      return `${is}${count}`;
    });
    rows.push(`| \`${rule.id}\` (${rule.basis}) | ${cells.join(" | ")} |`);
  }

  // The negative control: a harness that passes a candidate known to break a rule is itself broken.
  const controls = mustFail.flatMap((id) => LANGS.map((lang) => [id, lang, status[`${id} ${lang}`], byLang[lang]]));
  for (const [id, lang, is, result] of controls) {
    if (is === "FAIL" && result) console.log(`CONTROL_OK ${name} ${lang} ${id} fails, as it must`);
    else {
      console.log(`CONTROL_BROKEN ${name} ${lang} ${id} is ${result ? is : "without a result"}, it must FAIL`);
      reasons.unshift(`- **control broken:** \`${id}\` ${lang} must FAIL on this candidate`);
      totals.FAIL++;
    }
  }

  const verdict = totals.FAIL ? "FAIL" : "PASS";
  green &&= verdict === "PASS";
  const line = `VERDICT ${name} ${verdict}: ${totals.PASS} PASS, ${totals.FAIL} FAIL, ${totals.NOT_COVERED} NOT_COVERED (sha ${sha}, run ${run})`;
  console.log(line);
  const times = LANGS.map((lang) => (byLang[lang] ? `${lang} ${(byLang[lang].ms / 1000).toFixed(0)} s` : `${lang} none`));
  summary.push(
    `## ${line}`,
    "",
    `| rule | ${LANGS.join(" | ")} |`,
    `| --- |${" --- |".repeat(LANGS.length)}`,
    ...rows,
    "",
    `Suite times: ${times.join(", ")}`,
    "",
    ...reasons,
    "",
  );
}
writeFileSync(join(dir, "summary.md"), summary.join("\n"));
if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary.join("\n"));
process.exitCode = green ? 0 : 1;
