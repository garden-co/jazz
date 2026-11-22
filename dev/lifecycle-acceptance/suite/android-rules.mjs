// The fifth leg: jazz#6246's emulator test at android.json's pin, which prints one line per rule
// after `--- rules, jazz-kotlin-android <version>`: `RULE <id> PASS|FAIL|BLOCKED: <detail>`.
// BLOCKED (something it needed failed first) is a FAIL of the candidate, never a pass.

// The test's ids, as reported here (prefixed android.), and who decided each.
const TEST_RULES = [
  ["android.memory", "brief"],
  ["persist.reopen.bearer", "brief"],
  ["persist.reopen.device", "brief"],
  ["offline.device-open", "brief"],
  ["persist.killed-process", "brief"],
  ["sync.offline-write", "brief"],
  // #native-language-apis' store.second-open is these two; its store.reopen the next two.
  ["store.second-open", "pending"],
  ["store.second-open.process", "pending"],
  ["store.reopen", "pending"],
  ["store.killed-holder", "pending"],
  ["store.reopen-after-failed-open", "pending"],
  ["store.delete-reopen", "pending"],
  ["offline.bearer-open", "pending"], // observe only: out of beta.1
];
const NOT_ON_ANDROID = ["store.no-two-writers", "store.per-account"];

const id = (rule) => (rule.startsWith("android.") ? rule : `android.${rule}`);

export const ANDROID_RULES = [
  ...TEST_RULES.map(([rule, basis]) => ({ id: id(rule), basis, title: `${rule}, on an emulator` })),
  ...NOT_ON_ANDROID.map((rule) => ({ id: id(rule), basis: "pending", title: `${rule}: no such check on Android` })),
];

export function judgeAndroid(log, { aarSha256, version, code }) {
  const fail = (why) => ({ status: "FAIL", observed: "FAIL", why });
  const tested = new RegExp(`^testing jazz-kotlin-android ${version.replace(/\./g, "\\.")}: ${aarSha256} `, "m").test(log);
  const marker = `--- rules, jazz-kotlin-android ${version}`;
  const at = log.indexOf(marker);
  const lines = at < 0 ? [] : log.slice(at + marker.length).split("\n");
  const reported = new Map();
  for (const line of lines) {
    const m = line.match(/^RULE (\S+) (PASS|FAIL|BLOCKED):\s*(.*?)(?: \(observe only\))?\s*$/);
    if (m && !reported.has(m[1])) reported.set(m[1], { outcome: m[2], detail: m[3] });
  }
  return ANDROID_RULES.map(({ id: ruleId, basis }) => {
    if (!tested) return { id: ruleId, ...fail("the emulator test didn't test the candidate's AAR") };
    const rule = ruleId === "android.memory" ? ruleId : ruleId.slice("android.".length);
    if (NOT_ON_ANDROID.includes(rule)) return { id: ruleId, status: "NOT_COVERED", why: "the Android test has no such check" };
    const got = reported.get(rule);
    const observed = got?.outcome === "PASS" ? "PASS" : "FAIL";
    const why = got
      ? got.outcome === "PASS" ? undefined : `${got.outcome}: ${got.detail}`
      : `not reported (exit ${code}${at < 0 ? ", no rules summary" : ""})`;
    if (basis === "pending")
      return { id: ruleId, status: "NOT_COVERED", observed, why: `awaiting Anselm; observed ${got?.outcome ?? "nothing"}${why ? ` (${why})` : ""}` };
    return { id: ruleId, status: observed, observed, why };
  });
}
