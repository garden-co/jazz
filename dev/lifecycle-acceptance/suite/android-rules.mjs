// The fifth leg's rules, read from the log of dev/scripts/test-kotlin-android-emulator.sh (jazz#6246).
export const ANDROID_RULES = [
  { id: "android.memory", title: "memory storage and sync on an emulator" },
  { id: "android.write", title: "persistent storage opens, writes, closes and reads back on an emulator" },
  { id: "android.restart", title: "reads back after SIGKILL and a restart, and syncs, on an emulator" },
  { id: "android.one-store", title: "who may open and delete a held store, on an emulator" },
];

// What the restart phase logs before its store-sharing part.
const RESTARTED = [
  "ok: read from the store after the process was killed",
  "ok: the offline write synced to a second client",
];

/**
 * `predatesOneStore` (android.json): the pinned test asserts that two apps share a store, the
 * opposite of the one-store rule. That part is then neither a PASS nor a FAIL of the product.
 */
export function judgeAndroid(log, { aarSha256, code, predatesOneStore }) {
  const tested = log.includes(`testing ${aarSha256} `);
  const passed = (phase) => log.includes(`: ${phase} passed`);
  const failed = (phase) => log.includes(`: ${phase} FAILED`);
  const fail = (why) => ({ status: "FAIL", why });
  const gap = (why) => ({ status: "NOT_COVERED", why });
  const pass = { status: "PASS" };
  const phase = (name) =>
    passed(name) ? pass : fail(failed(name) ? `phase ${name} failed (see the job's log)` : `phase ${name} didn't run (exit ${code})`);

  if (!tested) return ANDROID_RULES.map(({ id }) => ({ id, ...fail("the emulator test didn't test the candidate's AAR") }));
  const restarted = RESTARTED.every((step) => log.includes(step));
  const blocked = "not reached: the phase before it failed";
  return [
    { id: "android.memory", ...phase("memory") },
    { id: "android.write", ...phase("write") },
    {
      id: "android.restart",
      ...(!passed("write") ? gap(blocked) : restarted ? pass : fail("no read and sync after the restart (see the job's log)")),
    },
    {
      id: "android.one-store",
      ...(predatesOneStore
        ? gap("the pinned test predates the one-store contract: it asserts that two apps share a store")
        : !passed("write") || !restarted
          ? gap(blocked)
          : phase("restart")),
    },
  ];
}
