// BigLabel (examples/big-label, Next.js + Better Auth): server and actions for
// big-label.storyboard.ts.
import { click, pointAt, sleep, type } from "../stage.mjs";
import { nextServer } from "../walkthrough.mjs";

const port = 3462;
const origin = `http://127.0.0.1:${port}`;
const run = Date.now().toString(36);
const email = (name) => `${name.toLowerCase()}-${run}@biglabel.test`;

export const app = "examples/big-label";
export const server = ({ dir }) => nextServer({ dir, port });

const nav = (page, name) => page.getByRole("link", { name, exact: true }).first();
const popover = (page) => page.locator("[popover]:popover-open");
const searchBox = (page) => page.getByPlaceholder(/Search/i).first();

export const actions = {
  async openSignIn({ page }) {
    await page.goto(origin);
    await page
      .getByRole("button", { name: "Create an account instead" })
      .waitFor({ timeout: 180_000 });
  },
  /** Off camera: a new account, with its own personal label. */
  async signUp({ page }, name) {
    if (page.url() === "about:blank") await page.goto(origin);
    await page.getByRole("button", { name: "Create an account instead" }).click();
    await page.getByLabel("Your name").fill(name);
    await page.getByLabel("Email").fill(email(name));
    await page.getByLabel("Password").fill("big-label-demo");
    await page.getByRole("button", { name: "Create account" }).click();
    await nav(page, "Overview").waitFor({ timeout: 120_000 });
  },
  async open({ page }, name, { after = 800 } = {}) {
    await click(page, nav(page, name), { after });
  },
  async loadDemoData({ page }) {
    await click(page, nav(page, "Settings"), { after: 800 });
    await click(page, page.getByText("Small", { exact: true }).first(), { after: 300 });
    await click(page, page.getByRole("button", { name: "Load demo data" }), { after: 300 });
    await page
      .getByText(/Loaded \d+ demo labels|already loaded/i)
      .first()
      .waitFor({ timeout: 120_000 });
  },
  /** Opens the label menu and waits for `label` in it. */
  async openLabelMenu({ page }, label) {
    const button = page.locator("nav button[aria-label]").first();
    await pointAt(page, button, { hover: 500 });
    if (!(await popover(page).count())) await button.click({ force: true });
    await popover(page).first().waitFor({ timeout: 10_000 });
    await sleep(500);
    if (label)
      await popover(page).getByText(label, { exact: true }).first().waitFor({ timeout: 30_000 });
  },
  async pickLabel({ page }, label, { after = 800 } = {}) {
    await click(page, popover(page).getByText(label, { exact: true }).first(), { after });
    if (await popover(page).count()) await page.keyboard.press("Escape");
  },
  async switchLabel(ctx, label) {
    await actions.openLabelMenu(ctx);
    await actions.pickLabel(ctx, label, { after: 1000 });
  },
  async search({ page }, text, { delay = 120 } = {}) {
    await type(page, searchBox(page), text, { delay });
  },
  async clearSearch({ page }) {
    await searchBox(page).fill("");
  },
  async openFirstRelease({ page }) {
    await click(page, page.locator("table tbody a").first(), { after: 1600 });
    await page.mouse.wheel(0, 300);
  },
  async fillNewMember({ page }, name, role) {
    await click(page, page.getByRole("button", { name: "Add member" }), { after: 500 });
    await type(page, page.getByRole("dialog").getByLabel("Email"), email(name), { delay: 25 });
    await click(page, page.getByRole("dialog").getByRole("combobox", { name: "Role" }), {
      after: 300,
    });
    await click(page, page.getByRole("option", { name: role }).first(), { after: 300 });
  },
  async addMember({ page }, name) {
    await click(page, page.getByRole("dialog").getByRole("button", { name: "Add member" }), {
      after: 1200,
    });
    await page.getByText(name).first().waitFor({ timeout: 30_000 });
  },
  async addArtist({ page }, name) {
    await click(page, page.getByRole("button", { name: "Add artist" }).first(), { after: 500 });
    await type(page, page.getByRole("dialog").getByLabel("Name"), name);
    await click(
      page,
      page
        .getByRole("dialog")
        .getByRole("button", { name: /Add|Save|Create/ })
        .last(),
      { after: 600 },
    );
  },
  async renameArtist({ page }, name) {
    await click(page, page.getByRole("button", { name: "Edit" }), { after: 500 });
    await type(page, page.getByRole("dialog").getByLabel("Name"), name, { clear: true });
    await click(
      page,
      page
        .getByRole("dialog")
        .getByRole("button", { name: /Save|Update/ })
        .last(),
      { after: 600 },
    );
    await page.getByRole("heading", { name }).waitFor();
  },
};
