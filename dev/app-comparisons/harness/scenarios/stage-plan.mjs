// StagePlan scenarios (SPEC.md). `ctx` is what the app's seed() returned:
// { showId, code } of "The Late Lanterns" (100 tasks; 3,000 in all).
import { nowInPage, summary, timeInPage, trackLoadedJs, waitInPage, armWhenInPage, SETTLE_MS } from "../lib.mjs";
import { phase } from "../watch.mjs";

const CARDS = "return document.querySelectorAll('[data-task-id]').length >= arg";
const cardWithText = "return [...document.querySelectorAll('[data-task-id]')].some((c) => c.textContent.includes(arg))";
const SUBMIT_TASK = `
  const input = document.querySelector('input[placeholder^="Add a task"]');
  input.closest('form').requestSubmit();
`;

const joinUrl = (url, ctx) => `${url}/#/join/${ctx.showId}/${ctx.code}`;

async function joinedPage(browser, url, ctx) {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(joinUrl(url, ctx));
  await waitInPage(page, CARDS, 100);
  return { context, page };
}

async function typeTask(page, title) {
  await page.getByPlaceholder(/^Add a task/).fill(title);
}

export async function run({ browser, url, ctx, runs, samples, log }) {
  const out = {};

  phase.name = "sp_join_board";
  // sp_join_board / sp_reload_board: fresh guest per run.
  const join = [];
  const reload = [];
  let loadedJsKb;
  for (let i = 0; i <= runs; i++) {
    const context = await browser.newContext();
    const page = await context.newPage();
    const loaded = trackLoadedJs(page);
    const t0 = Date.now();
    await page.goto(joinUrl(url, ctx));
    await waitInPage(page, CARDS, 100);
    const tJoin = Date.now() - t0;
    if (loadedJsKb === undefined && i === 1) loadedJsKb = loaded();
    // Let background work settle (e.g. a local-first upgrade) before the reload.
    await page.waitForTimeout(SETTLE_MS);
    const t1 = Date.now();
    await page.reload();
    await waitInPage(page, CARDS, 100);
    const tReload = Date.now() - t1;
    if (i > 0) {
      join.push(tJoin);
      reload.push(tReload);
    }
    log(`  join ${tJoin} ms, reload ${tReload} ms`);
    await context.close();
  }
  out.sp_join_board = summary(join);
  out.sp_reload_board = summary(reload);
  out.loaded_js_gzip_kb = loadedJsKb;

  phase.name = "sp_add_task";
  // Echo scenarios on one joined guest.
  const { context, page } = await joinedPage(browser, url, ctx);
  await page.waitForTimeout(SETTLE_MS);
  const add = [];
  for (let i = 0; i <= samples; i++) {
    const title = `Bench add ${Date.now()}-${i}`;
    await typeTask(page, title);
    const ms = await timeInPage(page, { act: SUBMIT_TASK, done: cardWithText, arg: title });
    if (i > 0) add.push(ms);
  }
  out.sp_add_task = summary(add);

  phase.name = "sp_move_done";
  // Move "In progress" cards to Done with the board's keyboard shortcut.
  const move = [];
  for (let i = 0; i <= samples; i++) {
    const id = await page.evaluate(() => document.querySelector('[data-status="doing"] [data-task-id]')?.getAttribute("data-task-id"));
    if (!id) break;
    const ms = await timeInPage(page, {
      arg: id,
      act: `
        const link = document.querySelector('[data-task-id="' + arg + '"] a') ?? document.querySelector('[data-task-id="' + arg + '"]');
        link.focus();
        link.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowRight', bubbles: true, cancelable: true }));
      `,
      done: `return !!document.querySelector('[data-status="done"] [data-task-id="' + arg + '"]')`,
    });
    if (i > 0) move.push(ms);
  }
  out.sp_move_done = summary(move);

  phase.name = "sp_cross_client";
  // sp_cross_client: guest A adds, guest B sees.
  const b = await joinedPage(browser, url, ctx);
  await b.page.waitForTimeout(SETTLE_MS);
  const cross = [];
  for (let i = 0; i <= samples; i++) {
    const title = `Bench cross ${Date.now()}-${i}`;
    await typeTask(page, title);
    const seen = (await armWhenInPage(b.page, { done: cardWithText, arg: title })).result;
    const sent = await page.evaluate((act) => {
      const t = performance.timeOrigin + performance.now();
      new Function(act)();
      return t;
    }, SUBMIT_TASK);
    const ms = (await seen) - sent;
    if (i > 0) cross.push(ms);
  }
  out.sp_cross_client = summary(cross);
  await b.context.close();
  await context.close();
  return out;
}
