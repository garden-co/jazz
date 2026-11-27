// Wequencer scenarios (SPEC.md). ctx from the app's seed(): { sessionId, onPads, users }.
// "Bench groove": 16 tracks x 64 steps = 1,024 pads, `onPads` of them on.
import { summary, timeInPage, waitInPage, armWhenInPage, trackLoadedJs, SETTLE_MS } from "../lib.mjs";
import { phase } from "../watch.mjs";

const PADS = 1024;
const PAD_SEL = "button.pad[aria-label*=', step ']";
const GRID_READY = `
  const pads = document.querySelectorAll("${PAD_SEL}");
  return pads.length >= ${PADS} && [...pads].filter((p) => p.getAttribute('aria-pressed') === 'true').length === arg;
`;
const padState = (index) => `document.querySelectorAll("${PAD_SEL}")[${index}].getAttribute('aria-pressed')`;

async function signedInPage(browser, app, url, ctx, user, open = true) {
  const context = await browser.newContext();
  const page = await context.newPage();
  await app.signIn(page, user, ctx);
  if (open) {
    await page.goto(`${url}/dashboard/${ctx.sessionId}`);
    await page.waitForFunction(`document.querySelectorAll("${PAD_SEL}").length >= ${PADS} && !document.querySelector("${PAD_SEL}:disabled")`, null, { timeout: 60_000, polling: "raf" });
  }
  return { context, page };
}

const onCount = (page) => page.evaluate((sel) => [...document.querySelectorAll(sel)].filter((p) => p.getAttribute("aria-pressed") === "true").length, PAD_SEL);

export async function run({ browser, app, url, ctx, runs, samples, log }) {
  const out = {};
  const sessionUrl = `${url}/dashboard/${ctx.sessionId}`;

  phase.name = "wq_open_pattern";
  // wq_open_pattern (fresh signed-in context) and wq_reload.
  const open = [];
  const reload = [];
  let loadedJsKb;
  for (let i = 0; i <= runs; i++) {
    const { context, page } = await signedInPage(browser, app, url, ctx, "a", false);
    const loaded = trackLoadedJs(page);
    const t0 = Date.now();
    await page.goto(sessionUrl);
    await waitInPage(page, GRID_READY, ctx.onPads);
    const tOpen = Date.now() - t0;
    if (i === 1) loadedJsKb = loaded();
    await page.waitForTimeout(SETTLE_MS);
    const t1 = Date.now();
    await page.reload();
    await waitInPage(page, GRID_READY, ctx.onPads);
    const tReload = Date.now() - t1;
    if (i > 0) {
      open.push(tOpen);
      reload.push(tReload);
    }
    log(`  open ${tOpen} ms, reload ${tReload} ms`);
    await context.close();
  }
  out.wq_open_pattern = summary(open);
  out.wq_reload = summary(reload);
  out.loaded_js_gzip_kb = loadedJsKb;

  const a = await signedInPage(browser, app, url, ctx, "a");
  const b = await signedInPage(browser, app, url, ctx, "b");
  await a.page.waitForTimeout(SETTLE_MS);

  phase.name = "wq_toggle_pad";
  // wq_toggle_pad: click a pad -> it shows the new state. Pads spread over the grid.
  const toggle = [];
  for (let i = 0; i <= samples; i++) {
    const index = (i * 37) % PADS;
    const ms = await timeInPage(a.page, {
      arg: index,
      act: `window.__before = ${padState("arg")}; document.querySelectorAll("${PAD_SEL}")[arg].click();`,
      done: `return ${padState("arg")} !== window.__before`,
    });
    if (i > 0) toggle.push(ms);
  }
  out.wq_toggle_pad = summary(toggle);

  phase.name = "wq_cross_client";
  // wq_cross_client: a clicks -> b sees the pad flip.
  const cross = [];
  for (let i = 0; i <= samples; i++) {
    const index = (i * 53 + 11) % PADS;
    const before = await b.page.evaluate(`${padState(index)}`);
    const seen = (await armWhenInPage(b.page, { arg: before, done: `return ${padState(index)} !== arg` })).result;
    const sent = await a.page.evaluate(`(() => { const t = performance.timeOrigin + performance.now(); document.querySelectorAll("${PAD_SEL}")[${index}].click(); return t; })()`);
    const ms = (await seen) - sent;
    if (i > 0) cross.push(ms);
    await a.page.waitForTimeout(50);
  }
  out.wq_cross_client = summary(cross);

  phase.name = "wq_concurrent";
  // wq_concurrent: a and b each toggle 32 pads at once (16 of them the same pad,
  // both pressing it from the same state); time until both grids show every
  // one of the 48 pads flipped.
  const conc = [];
  for (let i = 0; i <= Math.min(runs, 5); i++) {
    await a.page.waitForTimeout(500);
    const base = 200 + i * 64;
    const aIdx = Array.from({ length: 32 }, (_, k) => base + k);
    const bIdx = Array.from({ length: 32 }, (_, k) => base + 16 + k);
    const all = [...new Set([...aIdx, ...bIdx])];
    const expected = await a.page.evaluate(({ sel, all }) => all.map((n) => document.querySelectorAll(sel)[n].getAttribute("aria-pressed") !== "true"), { sel: PAD_SEL, all });
    const done = `return arg.all.every((n, k) => (document.querySelectorAll("${PAD_SEL}")[n].getAttribute('aria-pressed') === 'true') === arg.expected[k])`;
    const seenA = (await armWhenInPage(a.page, { arg: { all, expected }, done, timeoutMs: 60_000 })).result;
    const seenB = (await armWhenInPage(b.page, { arg: { all, expected }, done, timeoutMs: 60_000 })).result;
    const clicks = (idx) => `(() => { const t = performance.timeOrigin + performance.now(); for (const n of ${JSON.stringify(idx)}) document.querySelectorAll("${PAD_SEL}")[n].click(); return t; })()`;
    const [ta, tb] = await Promise.all([a.page.evaluate(clicks(aIdx)), b.page.evaluate(clicks(bIdx))]);
    const end = Math.max(await seenA, await seenB);
    const ms = end - Math.min(ta, tb);
    if (i > 0) conc.push(ms);
    log(`  concurrent ${Math.round(ms)} ms`);
  }
  out.wq_concurrent = summary(conc);
  await a.context.close();
  await b.context.close();
  return out;
}
