// BandBook (examples/band-book, Next.js + Better Auth): server and actions for
// band-book.storyboard.ts.
import { click, sleep } from "../stage.mjs";
import { nextServer } from "../walkthrough.mjs";

const port = 3461;
const origin = `http://127.0.0.1:${port}`;
const run = Date.now().toString(36);

export const app = "examples/band-book/apps/nextjs-betterauth";
export const server = ({ dir }) =>
  nextServer({
    dir,
    port,
    // The example's local development secrets, from .env.example.
    env: {
      BACKEND_SECRET: "band-book-development-backend-secret",
      BETTER_AUTH_SECRET: "band-book-local-development-better-auth-secret",
    },
  });

const side = (page, name) => page.getByRole("button", { name, exact: true }).first();
const title = (page) => page.getByRole("textbox", { name: "Page title" });
// The block editor's text areas, found by what they contain.
const textareaWith = async (page, needle, timeout = 30_000) => {
  const end = Date.now() + timeout;
  for (;;) {
    const i = await page
      .locator("textarea")
      .evaluateAll((els, n) => els.findIndex((e) => e.value.includes(n)), needle);
    if (i >= 0) return page.locator("textarea").nth(i);
    if (Date.now() > end) throw new Error(`No text block with ${needle}`);
    await sleep(200);
  }
};

export const actions = {
  /** Off camera: a new account; the server seeds a demo band. */
  async signUp({ page }, name) {
    await page.goto(origin);
    await page.getByRole("button", { name: "Create an account" }).click({ timeout: 180_000 });
    await page.getByLabel("Name").fill(name);
    await page.getByLabel("Email").fill(`${name.toLowerCase()}-${run}@band-book.test`);
    await page.getByLabel("Password").fill("correct horse battery");
    await page.getByRole("button", { name: "Create account" }).click();
    await page.waitForURL(/\/workspace/);
    await side(page, "Setlist: spring tour").waitFor();
  },
  async openPage({ page }, name, { after = 800 } = {}) {
    await click(page, side(page, name), { after });
  },
  async tick({ page }, name) {
    await click(page, page.getByRole("checkbox", { name }).first(), { after: 900 });
  },
  async addTextBlock({ page }, text) {
    await title(page).waitFor();
    await click(page, page.getByRole("button", { name: "Add block" }), { after: 400 });
    await click(page, page.getByRole("menuitem", { name: "Text" }), { after: 300 });
    await page.keyboard.type(text, { delay: 55 });
  },
  async openIssues({ page }) {
    await click(page, side(page, "Issues"), { after: 800 });
    await page.getByRole("table").waitFor();
  },
  async showBoardView({ page }) {
    await click(page, page.getByText("Board", { exact: true }).first(), { after: 600 });
  },
  async createEditLink({ page, state }) {
    await click(page, page.getByRole("button", { name: "Share" }), { after: 800 });
    await click(page, page.getByRole("button", { name: "Create link" }), { after: 800 });
    const linkInput = page.getByRole("dialog").getByRole("textbox").last();
    await linkInput.waitFor();
    state.inviteUrl = await linkInput.inputValue();
  },
  async closeShareDialog({ page }) {
    await click(page, page.getByRole("button", { name: "Done" }), { after: 500 });
  },
  async openEditLink({ stage, on, page, state }) {
    await page.goto(state.inviteUrl);
    await stage.recast(on);
    await page.waitForURL(/p=/);
    await title(page).waitFor();
  },
  /** Types at the end of the text block that contains `needle`. */
  async append({ page }, needle, text) {
    await click(page, await textareaWith(page, needle), { after: 200 });
    await page.keyboard.press("End");
    await page.keyboard.type(text, { delay: 70 });
  },
  async waitForText({ page }, needle) {
    await textareaWith(page, needle);
  },
  async expectNoText({ page }, needle) {
    const found = await page
      .locator("textarea")
      .evaluateAll((els, n) => els.some((e) => e.value.includes(n)), needle);
    if (found) throw new Error(`"${needle}" arrived while this laptop's Wi-Fi was off`);
  },
};
