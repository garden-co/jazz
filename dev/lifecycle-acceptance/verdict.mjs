// PASS / FAIL / NOT_COVERED per rule and language, from suite/run.mjs's results.
// usage: CANDIDATES='<json list>' node verdict.mjs <results dir>
// Fails closed: a language without results, or a rule without a result, is a FAIL. Without a FAIL,
// a rule that is undecided or covered elsewhere makes the verdict INCOMPLETE, never PASS.
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { LANGS } from "./lib/common.mjs";
import { ANDROID_RULES } from "./suite/android-rules.mjs";
import { RULES } from "./suite/rules/index.mjs";

const dir = process.argv[2];
// Rules a release ships failing, by decision: their FAILs are reported apart, and block nothing.
const known = JSON.parse(readFileSync(new URL("known-issues.json", import.meta.url), "utf8"));
const isKnown = (id) => known.rules.some((rule) => (rule.endsWith(".*") ? id.startsWith(rule.slice(0, -1)) : id === rule));
const candidates = JSON.parse(process.env.CANDIDATES ?? "[]");
if (!dir || !candidates.length) {
  console.log("FAIL verdict: no results dir or no candidates");
  process.exit(1);
}

const summary = [];
let green = true;
for (const { name, sha, run, mustFail = [] } of candidates) {
  const byLang = {};
  for (const lang of [...LANGS, "android"]) {
    const file = join(dir, `${name}-${lang}.json`);
    if (!existsSync(file)) continue;
    const result = JSON.parse(readFileSync(file, "utf8"));
    if (result.sha === sha && result.run === String(run) && result.lang === lang) byLang[lang] = result;
  }
  let open = 0; // rules nobody has decided or covered yet: no PASS over them
  const totals = { PASS: 0, FAIL: 0, NOT_COVERED: 0 };
  const knownFails = [];
  const blocking = [];
  const rows = [];
  const reasons = [];
  const status = {};
  for (const rule of RULES) {
    const cells = LANGS.map((lang) => {
      const got = byLang[lang]?.results.find((result) => result.id === rule.id);
      let is = got?.status ?? "FAIL";
      // A PASS is every round of the rule, not some.
      const short = is === "PASS" && !(got.passed >= rule.rounds);
      if (short || !(is in totals)) is = "FAIL";
      totals[is]++;
      // The only gap a PASS may have is one the rule declares for that language.
      const declared = !rule.external && rule.notCovered?.[lang];
      if (is === "NOT_COVERED" && !declared && !isKnown(rule.id)) open++;
      status[`${rule.id} ${lang}`] = got?.observed ?? is;
      if (!got) {
        console.log(`FAIL ${name} ${lang} ${rule.id}: no result (setup broke, see the job)`);
        blocking.push(`${rule.id} ${lang} (no result)`);
        return "FAIL (no result)";
      }
      const count = is === "NOT_COVERED" ? "" : ` ${got.passed}/${got.rounds}`;
      const worst = Object.entries(got.failures).sort((a, b) => b[1].count - a[1].count)[0];
      const why = short
        ? `only ${got.passed} of ${rule.rounds} rounds ran`
        : is === "FAIL" && worst
          ? `${worst[1].count}x ${worst[0]}`
          : got.why;
      const label = is === "FAIL" && isKnown(rule.id) ? "KNOWN_ISSUE" : is;
      if (label === "KNOWN_ISSUE") knownFails.push(`${rule.id} ${lang}`);
      else if (is === "FAIL") blocking.push(`${rule.id} ${lang}`);
      console.log(`${label} ${name} ${lang} ${rule.id}${count}${why ? `: ${why}` : ""}`);
      if (why && is !== "PASS") reasons.push(`- \`${rule.id}\` ${lang} ${is}: ${why.replace(/\s+/g, " ").slice(0, 300)}`);
      return `${label}${count}`;
    });
    rows.push(`| \`${rule.id}\` (${rule.basis}) | ${cells.join(" | ")} |`);
  }

  if (byLang.android)
    console.log(`NOTE ${name} android: the pinned test, jazz#6246 @ ${byLang.android.pinned}, is ${byLang.android.review ?? "an unreviewed test head"}`);
  const android = ANDROID_RULES.map(({ id, gap }) => {
    const got = byLang.android?.results.find((result) => result.id === id);
    const is = got?.status in totals ? got.status : "FAIL";
    const why = got ? got.why : "no result (setup broke, see the job)";
    totals[is]++;
    if (is === "NOT_COVERED" && !gap && !isKnown(id)) open++;
    status[`${id} android`] = got?.observed ?? is;
    const label = is === "FAIL" && got && isKnown(id) ? "KNOWN_ISSUE" : is;
    if (label === "KNOWN_ISSUE") knownFails.push(`${id} android`);
    else if (is === "FAIL") blocking.push(`${id} android`);
    console.log(`${label} ${name} android ${id}${why ? `: ${why}` : ""}`);
    if (why && is !== "PASS") reasons.push(`- \`${id}\` android ${is}: ${why}`);
    return `\`${id}\` ${label}`;
  });

  // What each leg resolved is the candidate's version: a fallback to a published release isn't.
  for (const lang of LANGS) {
    const result = byLang[lang];
    if (!result) continue;
    const seen = [...new Set((result.resolved ?? []).flatMap(({ what }) => [...what.matchAll(/v?(2\.\d+\.\d+-[a-z]+(?:\.\d+)*)/g)].map((m) => m[1])))];
    const ok = seen.length > 0 && seen.every((v) => v === result.version);
    console.log(`${ok ? "VERSION_OK" : "VERSION_WRONG"} ${name} ${lang} resolved ${seen.join(", ") || "nothing"} (candidate ${result.version})`);
    if (!ok) blocking.unshift(`${lang} resolved ${seen.join(", ") || "no version"}, not ${result.version}`);
  }
  if (byLang.android) {
    const ok = byLang.android.version === byLang[LANGS.find((l) => byLang[l])]?.version;
    console.log(`${ok ? "VERSION_OK" : "VERSION_WRONG"} ${name} android tested jazz-kotlin-android ${byLang.android.version}`);
    if (!ok) blocking.unshift(`android tested ${byLang.android.version}`);
  }

  // The negative control: a harness that passes a candidate known to break a rule is itself broken.
  const controls = mustFail.flatMap((id) =>
    (id.startsWith("android.") ? ["android"] : LANGS)
      // A language that declares it can't run the rule can't be its control.
      .filter((lang) => !RULES.find((rule) => rule.id === id)?.notCovered?.[lang])
      .map((lang) => [id, lang, status[`${id} ${lang}`], byLang[lang]]),
  );
  for (const [id, lang, is, result] of controls) {
    if (is === "FAIL" && result) console.log(`CONTROL_OK ${name} ${lang} ${id} fails, as it must`);
    else {
      console.log(`CONTROL_BROKEN ${name} ${lang} ${id} is ${result ? is : "without a result"}, it must FAIL`);
      reasons.unshift(`- **control broken:** \`${id}\` ${lang} must FAIL on this candidate`);
      totals.FAIL++;
      blocking.unshift(`control ${id} ${lang}`);
    }
  }

  const verdict = blocking.length ? "FAIL" : open ? "INCOMPLETE" : "PASS";
  console.log(`BLOCKING ${name} ${blocking.length}: ${blocking.join(", ") || "none"}`);
  console.log(`KNOWN_ISSUES ${name} ${knownFails.length}: ${knownFails.join(", ") || "none"}`);
  green &&= verdict === "PASS";
  const line = `VERDICT ${name} ${verdict}: ${totals.PASS} PASS, ${blocking.length} blocking FAIL, ${knownFails.length} known-issue FAIL, ${totals.NOT_COVERED} NOT_COVERED, ${open} of them open (sha ${sha}, run ${run})`;
  console.log(line);
  const times = LANGS.map((lang) => (byLang[lang] ? `${lang} ${(byLang[lang].ms / 1000).toFixed(0)} s` : `${lang} none`));
  summary.push(
    `## ${line}`,
    "",
    `| rule | ${LANGS.join(" | ")} |`,
    `| --- |${" --- |".repeat(LANGS.length)}`,
    ...rows,
    "",
    `Android (AAR on an emulator, jazz#6246 @ ${byLang.android?.pinned ?? "?"}${byLang.android ? `, ${byLang.android.review ?? "unreviewed test head"}` : ""}): ${android.join(", ")}`,
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
