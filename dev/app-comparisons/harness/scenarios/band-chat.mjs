// BandChat scenarios (SPEC.md). ctx from the app's seed(): { busyRoomId, quietRoomId, users }.
// Busy room: 2,000 messages; the UI shows the newest 200 (its HISTORY_PAGE).
import { summary, timeInPage, waitInPage, armWhenInPage, trackLoadedJs, SETTLE_MS } from "../lib.mjs";
import { phase } from "../watch.mjs";

const PAGE = 200;
const BUBBLES = "return document.querySelectorAll('[data-message-id]').length >= arg";
const bubbleWithText = "return [...document.querySelectorAll('[data-message-id]')].some((b) => b.textContent.includes(arg))";
// The composer's Send button (the UI's own submit path).
const SEND = `
  [...document.querySelectorAll('button')].find((b) => (b.getAttribute('aria-label') ?? b.textContent.trim()) === 'Send').click();
`;

async function signedInPage(browser, app, url, ctx, user, roomId) {
  const context = await browser.newContext();
  const page = await context.newPage();
  await app.signIn(page, user, ctx);
  if (roomId) {
    await page.goto(`${url}/?room=${roomId}`);
    await waitInPage(page, BUBBLES, PAGE);
  }
  return { context, page };
}

export async function run({ browser, app, url, ctx, runs, samples, log }) {
  const out = {};
  const room = `${url}/?room=${ctx.busyRoomId}`;

  phase.name = "bc_open_room";
  // bc_open_room (fresh signed-in context: nothing cached) and bc_resume (reload).
  const open = [];
  const resume = [];
  let loadedJsKb;
  for (let i = 0; i <= runs; i++) {
    const { context, page } = await signedInPage(browser, app, url, ctx, "a");
    const loaded = trackLoadedJs(page);
    const t0 = Date.now();
    await page.goto(room);
    await waitInPage(page, BUBBLES, PAGE);
    const tOpen = Date.now() - t0;
    if (i === 1) loadedJsKb = loaded();
    await page.waitForTimeout(SETTLE_MS);
    const t1 = Date.now();
    await page.reload();
    await waitInPage(page, BUBBLES, PAGE);
    const tResume = Date.now() - t1;
    if (i > 0) {
      open.push(tOpen);
      resume.push(tResume);
    }
    log(`  open ${tOpen} ms, resume ${tResume} ms`);
    await context.close();
  }
  out.bc_open_room = summary(open);
  out.bc_resume = summary(resume);
  out.loaded_js_gzip_kb = loadedJsKb;

  const a = await signedInPage(browser, app, url, ctx, "a", ctx.busyRoomId);
  await a.page.waitForTimeout(SETTLE_MS);

  phase.name = "bc_send";
  // bc_send: Enter in the composer -> the bubble is shown.
  const send = [];
  for (let i = 0; i <= samples; i++) {
    const text = `bench send ${Date.now()}-${i}`;
    await a.page.getByRole('textbox', { name: 'Message input' }).fill(text);
    const ms = await timeInPage(a.page, { act: SEND, done: bubbleWithText, arg: text });
    if (i > 0) send.push(ms);
  }
  out.bc_send = summary(send);

  phase.name = "bc_switch_room";
  // bc_switch_room: busy room -> another room via the URL's room link -> its messages.
  const switchMs = [];
  for (let i = 0; i <= samples; i++) {
    const target = i % 2 ? ctx.busyRoomId : ctx.quietRoomId;
    const ms = await timeInPage(a.page, {
      arg: target,
      act: `
        const link = document.querySelector('a[href*="room=' + arg + '"]');
        if (link) link.click(); else { history.pushState(null, '', '?room=' + arg); dispatchEvent(new PopStateEvent('popstate')); }
      `,
      done: `return new URLSearchParams(location.search).get('room') === arg && document.querySelectorAll('[data-message-id]').length >= (arg === '${ctx.busyRoomId}' ? ${PAGE} : 1) && !document.body.textContent.includes('Loading messages')`,
    });
    if (i > 0) switchMs.push(ms);
  }
  out.bc_switch_room = summary(switchMs);
  await a.page.goto(room);
  await waitInPage(a.page, BUBBLES, PAGE);

  phase.name = "bc_cross_client";
  // bc_cross_client: a sends -> b (same room) sees it.
  const b = await signedInPage(browser, app, url, ctx, "b", ctx.busyRoomId);
  await b.page.waitForTimeout(SETTLE_MS);
  const cross = [];
  for (let i = 0; i <= samples; i++) {
    const text = `bench cross ${Date.now()}-${i}`;
    await a.page.getByRole('textbox', { name: 'Message input' }).fill(text);
    const seen = (await armWhenInPage(b.page, { done: bubbleWithText, arg: text })).result;
    const sent = await a.page.evaluate((act) => {
      const t = performance.timeOrigin + performance.now();
      new Function(act)();
      return t;
    }, SEND);
    const ms = (await seen) - sent;
    if (i > 0) cross.push(ms);
  }
  out.bc_cross_client = summary(cross);
  await b.context.close();
  await a.context.close();
  return out;
}
