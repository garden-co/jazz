// Smoke test against a deployed copy of this app.
//   npm i --no-save playwright && npx playwright install chromium
//   BASE=https://<deployment> node smoke/smoke.mjs
// Optional: BASELINE_RED_EMAIL + BASELINE_RED_PASSWORD (a red user created by an earlier release's run)
// also check that its rows (red-team-63, red-private-63) survived.
// Exits 1 if any step fails. Creates fresh synthetic users each run.
import { chromium } from "playwright";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { randomBytes } from "node:crypto";
import { join } from "node:path";

const BASE = process.env.BASE;
if (!BASE) throw new Error("set BASE to the deployed URL");
// Fresh synthetic users get a random password for this run only.
const PW = `Smoke-${randomBytes(12).toString("base64url")}!`;
const BASELINE_PW = process.env.BASELINE_RED_PASSWORD;
const run = Date.now().toString(36);
const results = [];
const step = (name, ok, detail = "") => {
  results.push(ok);
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? "  — " + detail : ""}`);
};

async function browser() {
  const bypass = process.env.T4_VERCEL_BYPASS_SECRET; // Vercel preview protection (Protection Bypass for Automation)
  const ctx = await chromium.launchPersistentContext(mkdtempSync(join(tmpdir(), "smoke-")), {
    headless: true,
    ...(bypass
      ? {
          extraHTTPHeaders: {
            "x-vercel-protection-bypass": bypass,
            "x-vercel-set-bypass-cookie": "true",
          },
        }
      : {}),
  });
  const page = ctx.pages()[0] ?? (await ctx.newPage());
  page.on("pageerror", (e) => {
    console.log("  pageerror:", String(e).slice(0, 300));
    if (String(e).includes("invalid_link_challenge")) page.__linkErrs = (page.__linkErrs ?? 0) + 1;
  });
  return { ctx, page };
}
async function me(page, want = () => true, timeout = 60000) {
  try {
    await page.waitForFunction(
      (w) => {
        const el = document.querySelector('[data-testid="whoami"]');
        if (!el) return false;
        const j = JSON.parse(el.textContent);
        return j.status === "ready" && new Function("j", "return " + w)(j);
      },
      want.toString().replace(/^\(?\s*\w*\s*\)?\s*=>\s*/, ""),
      { timeout },
    );
  } catch {
    return null;
  }
  return JSON.parse(await page.textContent('[data-testid="whoami"]', { timeout: 2000 }));
}
async function rows(page, list) {
  return page.$$eval(`[data-testid="${list}-notes"] li`, (l) => l.map((x) => x.textContent));
}
async function waitRow(page, list, text, timeout) {
  return page
    .waitForFunction(
      ([l, t]) =>
        [...document.querySelectorAll(`[data-testid="${l}-notes"] li`)].some((li) =>
          li.textContent.endsWith(t),
        ),
      [list, text],
      { timeout },
    )
    .then(() => true)
    .catch(() => false);
}
async function write(page, kind, text) {
  await page.fill('[data-testid="text"]', text);
  await page.click(`[data-testid="add-${kind}"]`);
  const ok = await waitRow(page, kind, text, 30000);
  const err = await page.textContent('[data-testid="err"]', { timeout: 2000 }).catch(() => "");
  return { ok, err };
}
// No retries: a sign-up or sign-in that fails is a FAIL (Anselm: flakes must fail until fixed). jazz#5553 (on
// vercel() a guest-link challenge is signed per function instance) shows up as "pageerror: … invalid_link_challenge".
async function signUp(page, email, password = PW) {
  return signUpOnce(page, email, password);
}
async function signIn(page, email, password) {
  return signInOnce(page, email, password);
}
async function signUpOnce(page, email, password) {
  await page.fill('[data-testid="email"]', email);
  await page.fill('[data-testid="password"]', password);
  await page.click('[data-testid="sign-up"]');
  return me(page, (j) => j.kind === "backend");
}
async function signInOnce(page, email, password) {
  await page.waitForSelector('[data-testid="sign-in"]', { timeout: 60000 });
  await page.fill('[data-testid="email"]', email);
  await page.fill('[data-testid="password"]', password);
  await page.click('[data-testid="sign-in"]');
  return me(page, (j) => j.kind === "backend");
}
async function freshUser(email) {
  const b = await browser();
  await b.page.goto(BASE);
  await me(b.page);
  const u = await signUp(b.page, email);
  return { ...b, u };
}

const teamRow = `smoke-red-${run}`;
try {
  // 1. user A (team red) signs up and writes a team row
  const A = await freshUser(`red-smoke-a-${run}@example.com`);
  step(
    "1a user A signs up with claim team=red",
    A.u?.claims?.team === "red",
    JSON.stringify(A.u?.claims ?? null),
  );
  const wA = await write(A.page, "team", teamRow);
  step("1b user A writes a team row", wA.ok && !wA.err, wA.err);

  // 2. user B (also team red) reads it through the claims policy, live
  const B = await freshUser(`red-smoke-b-${run}@example.com`);
  step("2a user B signs up with claim team=red", B.u?.claims?.team === "red");
  step("2b user B sees A's row (team policy)", await waitRow(B.page, "team", teamRow, 20000));
  const live = `smoke-red-live-${run}`;
  await write(A.page, "team", live);
  step("2c user B gets A's next row live", await waitRow(B.page, "team", live, 20000));

  // 3. user C (team blue) must not see it
  const C = await freshUser(`blue-smoke-c-${run}@example.com`);
  step("3a user C signs up with claim team=blue", C.u?.claims?.team === "blue");
  const leaked = await waitRow(C.page, "team", teamRow, 8000);
  const cTeam = await rows(C.page, "team");
  step(
    "3b user C does not see red rows",
    !leaked && !cTeam.some((r) => r.startsWith("red|")),
    `C sees ${cTeam.length} team rows`,
  );
  const cPriv = await write(C.page, "private", `smoke-c-private-${run}`);
  step(
    "3c user C's own private row works, A doesn't see it",
    cPriv.ok && !(await waitRow(A.page, "private", `smoke-c-private-${run}`, 5000)),
  );

  // 4. a guest writes, then signs up and keeps account + row
  const G = await browser();
  await G.page.goto(BASE);
  const g = await me(G.page);
  step("4a guest starts on a device account", g?.kind === "device", g?.accountId);
  const gRow = `smoke-guest-${run}`;
  const wG = await write(G.page, "private", gRow);
  step("4b guest writes a private row", wG.ok && !wG.err, wG.err);
  const gTeam = await write(G.page, "team", `smoke-guest-team-${run}`);
  step(
    "4c guest (no claim) is refused a team row",
    !gTeam.ok && /denied/i.test(gTeam.err),
    gTeam.err.slice(0, 90),
  );
  const gu = await signUp(G.page, `guest-smoke-${run}@example.com`);
  step(
    "4d guest signs up, same accountId",
    gu?.kind === "backend" && gu?.accountId === g?.accountId,
    `${g?.accountId} -> ${gu?.accountId}`,
  );
  step("4e signed-up guest still sees its row", await waitRow(G.page, "private", gRow, 20000));
  await G.page.reload();
  const gr = await me(G.page);
  step(
    "4f after reload: still the same signed-in account and row",
    gr?.accountId === g?.accountId &&
      gr?.kind === "backend" &&
      (await waitRow(G.page, "private", gRow, 20000)),
  );

  // 5. optional: rows from earlier releases survived
  if (process.env.BASELINE_RED_EMAIL) {
    if (!BASELINE_PW) throw new Error("BASELINE_RED_EMAIL needs BASELINE_RED_PASSWORD");
    const R = await browser();
    await R.page.goto(BASE);
    await me(R.page);
    let r = await signIn(R.page, process.env.BASELINE_RED_EMAIL, BASELINE_PW).catch(() => null);
    if (r?.claims?.team !== "red" && process.env.BASELINE_SEED) {
      // First run with a new baseline account (BASELINE_SEED=1): create it and its rows; later runs check them.
      const S = await browser();
      await S.page.goto(BASE);
      await me(S.page);
      const s = await signUp(S.page, process.env.BASELINE_RED_EMAIL, BASELINE_PW);
      const t1 = await write(S.page, "team", "red-team-63");
      const t2 = await write(S.page, "private", "red-private-63");
      console.log(
        `SEEDED baseline user ${process.env.BASELINE_RED_EMAIL}: ${s?.claims?.team === "red" && t1.ok && t2.ok ? "ok" : "FAILED"}`,
      );
      await S.ctx.close();
      await R.page.goto(BASE);
      await me(R.page);
      r = await signIn(R.page, process.env.BASELINE_RED_EMAIL, BASELINE_PW).catch(() => null);
    }
    step("5a baseline red user signs in", r?.claims?.team === "red");
    step(
      "5b baseline rows survived (red-team-63, red-private-63)",
      (await waitRow(R.page, "team", "red-team-63", 20000)) &&
        (await waitRow(R.page, "private", "red-private-63", 20000)),
    );
    const t = await rows(R.page, "team");
    step(
      "5c baseline red user sees no blue rows",
      t.length > 0 && t.every((x) => x.startsWith("red|")),
    );
    await R.ctx.close();
  }
  for (const x of [A, B, C, G]) await x.ctx.close();
} catch (e) {
  step("unexpected error", false, String(e).slice(0, 400));
}
const failed = results.filter((x) => !x).length;
console.log(
  `\n${failed === 0 ? "PASS" : "FAIL"}: ${results.length - failed}/${results.length} steps passed (${BASE}, run ${run})`,
);
process.exit(failed === 0 ? 0 : 1);
