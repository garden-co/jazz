// Jamazon (examples/jamazon, Next.js + Better Auth): server and actions for
// jamazon.storyboard.ts.
import { randomBytes } from "node:crypto";
import { click, sleep, type } from "../stage.mjs";
import { nextServer } from "../walkthrough.mjs";

const port = 3466;
const origin = `http://127.0.0.1:${port}`;
const email = `ada-${Date.now().toString(36)}@jamazon.test`;
const password = "jamazon-demo-2026";
const PRODUCT = "Brass snare, 14 × 5.5 in";
const secret = () => randomBytes(32).toString("base64url");

export const app = "examples/jamazon/apps/nextjs-betterauth";
export const server = ({ dir }) =>
  nextServer({
    dir,
    port,
    // Throwaway local secrets, as `pnpm dev` would generate; sandbox payments.
    env: { BACKEND_SECRET: secret(), BETTER_AUTH_SECRET: secret() },
  });

const quantity = (page) =>
  page.getByRole("spinbutton", { name: `Quantity of ${PRODUCT}`, exact: true });
const hasQuantity = async (page, n) => (await quantity(page).inputValue()) === String(n);
const until = async (check, ms = 30_000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await check().catch(() => false)) return true;
    await sleep(250);
  }
  return false;
};
const leftSignIn = (url) => !url.pathname.startsWith("/sign-in");

export const actions = {
  async compileRoutes({ page }, paths) {
    for (const path of paths) {
      await page.goto(origin + path);
      await page.getByRole("heading").first().waitFor({ timeout: 240_000 });
    }
  },
  async createAccount({ page }, name) {
    await page.goto(`${origin}/sign-in`);
    await page.getByText("Create account", { exact: true }).first().click();
    await page.getByLabel("Name").fill(name);
    await page.getByLabel("Email").fill(email);
    await page.getByLabel("Password").fill(password);
    await page.getByRole("button", { name: "Create account" }).last().click();
    await page.waitForURL(leftSignIn, { timeout: 120_000 });
  },
  async signIn({ page }) {
    await page.goto(`${origin}/sign-in`);
    await page.getByLabel("Email").fill(email);
    await page.getByLabel("Password").fill(password);
    await page.getByRole("button", { name: "Sign in" }).last().click();
    await page.waitForURL(leftSignIn, { timeout: 120_000 });
  },
  async openCart({ page }) {
    await page.goto(`${origin}/cart`);
    await page.getByText("Your cart is empty").waitFor({ timeout: 120_000 });
  },
  async openStore({ page }) {
    await page.goto(origin);
    await page.getByPlaceholder("Search the store").waitFor({ timeout: 120_000 });
  },
  async search({ page }, text) {
    await type(page, page.getByPlaceholder("Search the store"), text, { delay: 160 });
    await page.getByRole("link", { name: PRODUCT }).waitFor();
  },
  async openProduct({ page }) {
    await click(page, page.getByRole("link", { name: PRODUCT }), { after: 800, direct: true });
    await page.getByRole("button", { name: "Add to cart" }).waitFor();
  },
  async addToCart({ page }) {
    await click(page, page.getByRole("button", { name: "Add to cart" }), { after: 300 });
  },
  async waitForCartItem({ page }) {
    await quantity(page).waitFor({ timeout: 30_000 });
  },
  async openCartLink({ page }) {
    await click(page, page.getByRole("link", { name: /^Cart/ }).first(), { after: 800 });
    await quantity(page).waitFor();
  },
  /** Steps the quantity with the + and − buttons, as a person would. */
  async setQuantity({ page }, n) {
    for (let i = 0; i < 20; i++) {
      const now = Number(await quantity(page).inputValue());
      if (now === n) return;
      const step = now < n ? "Increment" : "Decrement";
      await click(page, page.getByRole("button", { name: `${step} Quantity of ${PRODUCT}` }), {
        after: 350,
        direct: true,
      });
    }
  },
  async waitForQuantity({ page }, n, ms = 30_000) {
    if (!(await until(() => hasQuantity(page, n), ms)))
      throw new Error(`The quantity never reached ${n} here`);
  },
  async expectQuantity({ page }, n) {
    if (!(await hasQuantity(page, n)))
      throw new Error(`Expected quantity ${n}: an edit arrived early`);
  },
  // Through the phone's menu: a client-side navigation keeps the session warm, so the
  // phone never flashes the signed-out state a full page load shows while auth resolves.
  async openOrdersFromMenu({ page }) {
    await click(
      page,
      page
        .getByRole("button", { name: /navigation|menu/i })
        .filter({ visible: true })
        .first(),
      { after: 500 },
    );
    await click(
      page,
      page.getByRole("link", { name: "Orders", exact: true }).filter({ visible: true }).first(),
      { after: 300 },
    );
    await page.getByRole("heading", { name: "Orders", exact: true }).first().waitFor();
  },
  /** Fills the shipping form. */
  async checkOut({ page }) {
    await click(
      page,
      page
        .getByRole("link", { name: "Check out" })
        .or(page.getByRole("button", { name: "Check out" }))
        .first(),
      { after: 800 },
    );
    await type(page, page.getByLabel("Full name"), "Ada Lovelace", { delay: 30 });
    await type(page, page.getByRole("textbox", { name: /^Address/ }).first(), "12 Harbour Street", {
      delay: 30,
    });
    await type(page, page.getByLabel("City"), "Bristol", { delay: 30 });
    await type(page, page.getByLabel("Postcode"), "BS1 4QA", { delay: 30 });
    await type(page, page.getByLabel("Country"), "United Kingdom", { delay: 30 });
  },
  async placeOrder({ page }) {
    await click(page, page.getByRole("button", { name: "Continue to review" }), { after: 900 });
    await click(page, page.getByRole("button", { name: "Place order" }), { after: 300 });
    await page.waitForURL(/\/orders\//, { timeout: 60_000 });
  },
  /** Picks a sandbox card outcome ("Approve" or "Decline") and pays. */
  async pay({ page }, outcome) {
    await click(page, page.getByText(outcome, { exact: true }), { after: 300 });
    await click(page, page.getByRole("button", { name: /^Pay / }), { after: 300 });
  },
};
