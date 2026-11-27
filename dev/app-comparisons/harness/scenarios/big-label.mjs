// BigLabel scenarios (SPEC.md). ctx from the app's seed():
// { labelId, secondLabelId, renameReleaseId, statusReleaseId, users }.
// Label 0 ("Northwind Records") and label 1 ("Low Tide Records") hold 512
// releases each, out of 4,096 over 8 labels. Expected rows come from the
// fixture: catalogue numbers are unique per label and release dates are
// unique per label, so every stack shows the same rows in the same order.
import { summary, timeInPage, waitInPage, armWhenInPage, trackLoadedJs, SETTLE_MS } from "../lib.mjs";
import { phase } from "../watch.mjs";
import {
  bigLabelFixture,
  labelCounts,
  releasesNewestFirst,
  BENCH_LABEL,
  SECOND_LABEL,
  PAGE_SIZE,
  SEARCHES,
  releaseStatuses,
} from "../../fixtures/big-label.mjs";

const fixture = bigLabelFixture();
const labelName = (key) => fixture.labels.find((l) => l.key === key).name;
const all = releasesNewestFirst(fixture, BENCH_LABEL);
const pageOf = (rows, page) => rows.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE).map((r) => r.catalogNumber);
const PAGES = Math.ceil(all.length / PAGE_SIZE);

// Release rows: table rows whose first cell links "#/releases/<id>"; the third
// cell (Title, Artist, Cat. no., Release date, Status) is the catalogue number.
const ROWS_ARE = `
  const rows = [...document.querySelectorAll('tr')].filter((tr) => tr.querySelector('td a[href^="#/releases/"]'));
  const nums = rows.map((tr) => tr.querySelectorAll('td')[2]?.textContent.trim());
  return nums.length === arg.length && nums.every((n, i) => n === arg[i]);
`;
const SEARCH_INPUT = 'input[placeholder="Search title or catalogue number"]';
const NEXT_PAGE = 'button[aria-label="Go to next page"]';
// React-controlled input: set the value the way a paste does, then fire `input`.
const typeInto = (sel, valueExpr) => `
  const input = document.querySelector('${sel}');
  Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, ${valueExpr});
  input.dispatchEvent(new Event('input', { bubbles: true }));
`;
// Overview: the h1 is the label's name; each stat is an h2 right after its label text.
const OVERVIEW_IS = `
  const stat = (label) => [...document.querySelectorAll('h2')].find((h) => h.previousElementSibling?.textContent === label)?.textContent;
  return document.querySelector('h1')?.textContent === arg.name && stat('Artists') === arg.artists &&
    stat('Releases') === arg.releases && stat('Members') === arg.members && stat('Teams') === arg.teams;
`;
const overview = (key) => {
  const c = labelCounts(fixture, key);
  return { name: labelName(key), artists: String(c.artists), releases: String(c.releases), members: String(c.members), teams: String(c.teams) };
};
// Release page: the "Status" entry of the metadata list.
const STATUS_IS = `return [...document.querySelectorAll('dt')].find((dt) => dt.textContent === 'Status')?.nextElementSibling?.textContent.trim() === arg`;
const sentence = (s) => s[0].toUpperCase() + s.slice(1);

/** A signed-in context with label 0 chosen in the switcher (the app remembers it in localStorage). */
async function signedInPage(browser, app, ctx, user) {
  const context = await browser.newContext();
  const page = await context.newPage();
  await app.signIn(page, user, ctx);
  await page.addInitScript((id) => localStorage.setItem("big-label-organization", id), ctx.labelId);
  return { context, page };
}

/** Opens the release's Edit dialog, sets `fields` (Title text, Status option); untimed. */
async function editRelease(page, { title, status }) {
  await page.getByRole("button", { name: "Edit", exact: true }).click();
  const dialog = page.locator("dialog[open]");
  await dialog.getByRole("heading", { name: "Edit release" }).waitFor();
  // Form fields are found by their <label> (its text starts with the field's name).
  const field = async (name) => {
    const id = await dialog.evaluate((d, name) => [...d.querySelectorAll("label")].find((l) => l.textContent.startsWith(name)).htmlFor, name);
    return page.locator(`[id="${id}"]`);
  };
  if (title !== undefined) await (await field("Title")).fill(title);
  if (status !== undefined) {
    await (await field("Status")).click();
    await page.getByRole("option", { name: sentence(status), exact: true }).click();
  }
}
const SAVE = `[...document.querySelectorAll('dialog[open] button')].find((b) => b.textContent.trim() === 'Save').click();`;

export async function run({ browser, app, url, ctx, runs, samples, log }) {
  const out = {};
  const firstPage = pageOf(all, 1);

  phase.name = "bl_open_releases";
  // bl_open_releases (fresh signed-in context) and bl_reload.
  const open = [];
  const reload = [];
  let loadedJsKb;
  for (let i = 0; i <= runs; i++) {
    const { context, page } = await signedInPage(browser, app, ctx, "a");
    const loaded = trackLoadedJs(page);
    const t0 = Date.now();
    await page.goto(`${url}/#/releases`);
    await waitInPage(page, ROWS_ARE, firstPage);
    const tOpen = Date.now() - t0;
    if (i === 1) loadedJsKb = loaded();
    await page.waitForTimeout(SETTLE_MS);
    const t1 = Date.now();
    await page.reload();
    await waitInPage(page, ROWS_ARE, firstPage);
    const tReload = Date.now() - t1;
    if (i > 0) {
      open.push(tOpen);
      reload.push(tReload);
    }
    log(`  open ${tOpen} ms, reload ${tReload} ms`);
    await context.close();
  }
  out.bl_open_releases = summary(open);
  out.bl_reload = summary(reload);
  out.loaded_js_gzip_kb = loadedJsKb;

  const a = await signedInPage(browser, app, ctx, "a");
  const b = await signedInPage(browser, app, ctx, "b");
  for (const { page } of [a, b]) {
    await page.goto(`${url}/#/releases`);
    await waitInPage(page, ROWS_ARE, firstPage);
  }
  await a.page.waitForTimeout(SETTLE_MS);

  phase.name = "bl_search";
  // bl_search: enter a term (a different title word each sample) -> the first
  // page of matching releases; then clear it (untimed) -> the unfiltered page.
  const search = [];
  for (let i = 0; i <= samples; i++) {
    const term = SEARCHES[i % SEARCHES.length];
    const ms = await timeInPage(a.page, {
      arg: pageOf(releasesNewestFirst(fixture, BENCH_LABEL, term), 1),
      act: typeInto(SEARCH_INPUT, JSON.stringify(term)),
      done: ROWS_ARE,
    });
    if (i > 0) search.push(ms);
    await timeInPage(a.page, { arg: firstPage, act: typeInto(SEARCH_INPUT, '""'), done: ROWS_ARE });
  }
  out.bl_search = summary(search);

  phase.name = "bl_next_page";
  // bl_next_page: "Go to next page", page after page (each one not shown before).
  const next = [];
  let current = 1;
  for (let i = 0; i <= samples; i++) {
    if (current + 1 > PAGES) {
      await a.page.evaluate(() => (window.location.hash = "#/"));
      await a.page.evaluate(() => (window.location.hash = "#/releases"));
      await waitInPage(a.page, ROWS_ARE, firstPage);
      current = 1;
    }
    const ms = await timeInPage(a.page, {
      arg: pageOf(all, current + 1),
      act: `document.querySelector('${NEXT_PAGE}').click()`,
      done: ROWS_ARE,
    });
    current++;
    if (i > 0) next.push(ms);
  }
  out.bl_next_page = summary(next);

  phase.name = "bl_switch_label";
  // bl_switch_label: pick label 1 in the side nav's label menu -> its overview
  // counts; switching back to label 0 is untimed.
  const first = overview(BENCH_LABEL);
  const second = overview(SECOND_LABEL);
  await a.page.evaluate(() => (window.location.hash = "#/"));
  await waitInPage(a.page, OVERVIEW_IS, first);
  const pickLabel = (name) => `[...document.querySelectorAll('[role=menuitem]')].find((m) => m.querySelector('span span')?.textContent === ${JSON.stringify(name)}).click()`;
  const openMenu = async () => {
    await a.page.evaluate(() => {
      const button = document.querySelector('button[aria-label="Open menu"]');
      if (button.getAttribute("aria-expanded") !== "true") button.click();
    });
    await a.page.waitForSelector("[role=menuitem]", { state: "visible" });
  };
  const switchLabel = [];
  for (let i = 0; i <= samples; i++) {
    await openMenu();
    const ms = await timeInPage(a.page, { arg: second, act: pickLabel(second.name), done: OVERVIEW_IS });
    if (i > 0) switchLabel.push(ms);
    await openMenu();
    await timeInPage(a.page, { arg: first, act: pickLabel(first.name), done: OVERVIEW_IS });
  }
  out.bl_switch_label = summary(switchLabel);

  phase.name = "bl_update_status";
  // bl_update_status: on a release's page, Edit -> Status -> Save (the timed
  // click) -> the page shows the new status.
  await a.page.evaluate((id) => (window.location.hash = `#/releases/${id}`), ctx.statusReleaseId);
  const statusKey = releasesNewestFirst(fixture, BENCH_LABEL)[1];
  let status = statusKey.status;
  await waitInPage(a.page, STATUS_IS, sentence(status));
  await a.page.waitForTimeout(SETTLE_MS);
  const update = [];
  for (let i = 0; i <= samples; i++) {
    status = releaseStatuses[(releaseStatuses.indexOf(status) + 1) % releaseStatuses.length];
    await editRelease(a.page, { status });
    const ms = await timeInPage(a.page, { arg: sentence(status), act: SAVE, done: STATUS_IS });
    if (i > 0) update.push(ms);
    await a.page.waitForSelector("dialog[open]", { state: "detached" }).catch(() => {});
  }
  out.bl_update_status = summary(update);

  phase.name = "bl_cross_client";
  // bl_cross_client: a renames the newest release (Edit -> Title -> Save, the
  // timed click) -> b's Releases page (label 0, page 1) shows the new title.
  await a.page.evaluate((id) => (window.location.hash = `#/releases/${id}`), ctx.renameReleaseId);
  await a.page.getByRole("button", { name: "Edit", exact: true }).waitFor();
  await b.page.waitForTimeout(SETTLE_MS);
  const titleIs = `return document.querySelector('td a[href="#/releases/${ctx.renameReleaseId}"]')?.textContent === arg`;
  const cross = [];
  for (let i = 0; i <= samples; i++) {
    const title = `Renamed release ${i}`;
    await editRelease(a.page, { title });
    const seen = (await armWhenInPage(b.page, { arg: title, done: titleIs })).result;
    const sent = await a.page.evaluate(`(() => { const t = performance.timeOrigin + performance.now(); ${SAVE} return t; })()`);
    const ms = (await seen) - sent;
    if (i > 0) cross.push(ms);
    await a.page.waitForSelector("dialog[open]", { state: "detached" }).catch(() => {});
    await a.page.waitForTimeout(50);
  }
  out.bl_cross_client = summary(cross);
  await a.context.close();
  await b.context.close();
  return out;
}
