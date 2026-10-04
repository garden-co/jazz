// PosterShop (examples/poster-shop, Next.js + Better Auth): server and actions
// for poster-shop.storyboard.ts. Shapes are remembered by name in `state`:
// "sun" (the demo poster's sun) and "added" (the rectangle Ada adds).
import { click, pointAt, sleep, type } from "../stage.mjs";
import { nextServer } from "../walkthrough.mjs";

const port = 3464;
const origin = `http://127.0.0.1:${port}`;
const uniq = Date.now().toString(36);

export const app = "examples/poster-shop/apps/nextjs-betterauth";
export const server = ({ dir }) =>
  nextServer({
    dir,
    port,
    // The example's local development secrets, from .env.example.
    env: {
      BACKEND_SECRET: "poster-shop-development-backend-secret",
      BETTER_AUTH_SECRET: "2SNhYRceYvKf1HnJ7mQxB3aWd6LeP9tR4uCg8Vz0Ds5FiOoAbXkMwZq",
    },
  });

const canvas = (page) => page.getByRole("application", { name: "Poster canvas" });
const shape = (page, id) => canvas(page).locator(`[data-shape-id="${id}"]`);
const swatch = (page, name) =>
  page
    .getByRole("radio", { name, exact: true })
    .or(page.getByRole("button", { name, exact: true }))
    .first();
const html = (page, id) => shape(page, id).evaluate((el) => el.outerHTML);

export const actions = {
  /** Off camera: a new account, straight into the poster editor. */
  async signUp({ page }, name) {
    await page.goto(origin);
    await page.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 240_000 });
    await page.getByRole("button", { name: "Create an account" }).click();
    await page.getByLabel("Name").fill(name);
    await page.getByLabel("Email").fill(`${name.toLowerCase()}-${uniq}@poster.test`);
    await page.getByLabel("Password").fill("poster-shop-demo");
    await page.getByRole("button", { name: "Create account" }).click();
    await canvas(page).waitFor({ timeout: 120_000 });
  },
  /** Selects the sun: the inner ellipse of the artwork layer. */
  async selectSun({ page, state }) {
    state.sun = await canvas(page)
      .locator('[data-kind="ellipse"]')
      .nth(1)
      .getAttribute("data-shape-id");
    await click(page, shape(page, state.sun), { after: 500 });
  },
  async pickColour({ page }, name, { after = 300 } = {}) {
    await click(page, swatch(page, name), { after });
  },
  async addRectangle({ page, state }) {
    await click(page, page.getByRole("button", { name: "Add rectangle" }), { after: 500 });
    state.added = await canvas(page)
      .locator('[data-kind="rect"]')
      .last()
      .getAttribute("data-shape-id");
  },
  /** Drags a shape by its centre, the way a person would. */
  async dragShape({ page, state }, name, dx, dy) {
    const box = await pointAt(page, shape(page, state[name]));
    await page.mouse.down();
    await page.mouse.move(box.x + box.width / 2 + dx, box.y + box.height / 2 + dy, { steps: 18 });
    await page.mouse.up();
    await sleep(400);
  },
  async selectShape({ page, state }, name) {
    await click(page, shape(page, state[name]), { after: 300 });
  },
  async createInviteLink({ page, state }) {
    await click(page, page.getByRole("button", { name: "Invite" }), { after: 600 });
    await click(page, page.getByRole("button", { name: "Create invite link" }), { after: 600 });
    state.link = await page.getByLabel("Invite link").inputValue();
  },
  async press({ page }, key) {
    await page.keyboard.press(key);
  },
  async openInviteLink({ stage, on, page, state }) {
    // The link differs from Grace's page only by its #fragment; load it fresh.
    await page.goto("about:blank");
    await page.goto(state.link);
    await stage.recast(on);
    await shape(page, state.sun).waitFor({ timeout: 90_000 });
  },
  /** Moves the cursor around the sun, so it shows on the other laptop. */
  async hoverSun({ page, state }) {
    const box = await shape(page, state.sun).boundingBox();
    for (const [dx, dy] of [
      [-60, -40],
      [40, -10],
      [10, 40],
    ])
      await page.mouse.move(box.x + box.width / 2 + dx, box.y + box.height / 2 + dy, {
        steps: 10,
      });
  },
  /** Recolours the sun, noting its colour on the `watcher` device first. */
  async recolourSun({ page, pages, state }, colour, watcher) {
    state.sunBefore = await shape(pages[watcher], state.sun).getAttribute("fill");
    await click(page, shape(page, state.sun), { after: 400 });
    await click(page, swatch(page, colour), { after: 300 });
  },
  async waitForSunChange({ page, state }) {
    for (
      let i = 0;
      i < 60 && (await shape(page, state.sun).getAttribute("fill")) === state.sunBefore;
      i++
    )
      await sleep(250);
  },
  async rememberShape({ page, state }, name) {
    state[`${name}Before`] = await html(page, state[name]);
  },
  async expectShapeUnchanged({ page, state }, name) {
    if ((await html(page, state[name])) !== state[`${name}Before`])
      throw new Error("An offline edit reached this laptop while the other's Wi-Fi was off");
  },
  async waitForShapeChange({ page, state }, name) {
    const before = state[`${name}Before`];
    for (let i = 0; i < 120 && (await html(page, state[name])) === before; i++) await sleep(250);
    if ((await html(page, state[name])) === before)
      throw new Error("The offline edits never arrived");
  },
  async saveCheckpoint({ page }, name) {
    await click(
      page,
      page
        .getByRole("tab", { name: "History" })
        .or(page.getByText("History", { exact: true }))
        .first(),
      { after: 600 },
    );
    await type(page, page.getByLabel("Checkpoint name"), name);
    await click(page, page.getByRole("button", { name: "Save" }), { after: 1000 });
  },
};
