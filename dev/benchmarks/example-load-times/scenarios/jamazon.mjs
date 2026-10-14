// Jamazon (Next.js + Better Auth): anonymous store load, account creation,
// a second device's cart, return visit to the store, cart reload.
import { device, random, rateLimitGate, visible } from "../harness.mjs";

const password = "jamazon-demo-2026";
const PRODUCT = "Brass snare, 14 × 5.5 in";
const product = (page) => page.locator('a[href^="/product/"]');
const quantity = (page) =>
  page.getByRole("spinbutton", { name: `Quantity of ${PRODUCT}`, exact: true });
const leftSignIn = (url) => !url.pathname.startsWith("/sign-in");

export default function jamazon({ server, browser }) {
  const { origin } = server;
  const signUpGate = rateLimitGate();
  const signInGate = rateLimitGate();
  const devices = new Set();
  const open = async (options) => {
    const d = await device(browser, options);
    devices.add(d);
    return d;
  };
  const close = async (d) => {
    devices.delete(d);
    await d?.close();
  };
  const email = () => `ada-${random()}@jamazon.test`;
  /** Browses the store anonymously, then fills the sign-up form. */
  const fillSignUp = async ({ page }, address) => {
    await page.goto(origin);
    await visible(product(page), { timeout: 120_000 });
    await page.goto(`${origin}/sign-in`);
    await page.getByText("Create account", { exact: true }).first().click();
    await page.getByLabel("Name").fill("Ada");
    await page.getByLabel("Email").fill(address);
    await page.getByLabel("Password").fill(password);
    await signUpGate();
  };
  const submitSignUp = async ({ page }) => {
    await page.getByRole("button", { name: "Create account" }).last().click();
    await page.waitForURL(leftSignIn, { timeout: 120_000 });
    await visible(product(page).or(page.getByText("Your cart is empty")), { timeout: 120_000 });
  };
  const signIn = async (address) => {
    const d = await open({ viewport: { width: 390, height: 800 } });
    const { page } = d;
    await page.goto(`${origin}/sign-in`);
    await page.getByLabel("Email").fill(address);
    await page.getByLabel("Password").fill(password);
    await signInGate();
    await page.getByRole("button", { name: "Sign in" }).last().click();
    await page.waitForURL(leftSignIn, { timeout: 120_000 });
    return d;
  };
  let customer, customerEmail;

  return {
    // A customer with the snare in their cart, on the cart page.
    async setup() {
      customer = await open();
      customerEmail = email();
      await fillSignUp(customer, customerEmail);
      await submitSignUp(customer);
      const { page } = customer;
      await page.goto(origin);
      // Wait for products (a hydrated store) before typing; text typed earlier is lost.
      await visible(product(page), { timeout: 120_000 });
      const box = page.getByPlaceholder("Search the store");
      for (let i = 0; i < 3; i++) {
        await box.fill("");
        await box.pressSequentially("snare", { delay: 120 });
        const found = await page
          .getByText(PRODUCT, { exact: true })
          .first()
          .waitFor({ timeout: 10_000 })
          .then(
            () => true,
            () => false,
          );
        if (found) break;
      }
      await page.getByText(PRODUCT, { exact: true }).first().click();
      await page.getByRole("button", { name: "Add to cart", exact: true }).click();
      await visible(page.getByRole("button", { name: "Add more to cart", exact: true }));
      // A full navigation right after the click can drop the cart write, so
      // go to the cart in-app, then reload once to check that it was stored.
      await page.getByRole("link", { name: "Cart" }).first().click();
      await visible(quantity(page));
      await page.reload();
      await visible(quantity(page));
    },
    scenarios: {
      cold_search: {
        prepare: async () => ({ d: await open() }),
        async run({ d }) {
          await d.page.goto(origin);
          await visible(d.page.getByPlaceholder("Search the store"), { timeout: 120_000 });
        },
        finish: ({ d }) => close(d),
      },
      cold_first_product: {
        prepare: async () => ({ d: await open() }),
        async run({ d }) {
          await d.page.goto(origin);
          await visible(product(d.page), { timeout: 120_000 });
        },
        finish: ({ d }) => close(d),
      },
      cold_signup: {
        async prepare() {
          const d = await open();
          await fillSignUp(d, email());
          return { d };
        },
        run: ({ d }) => submitSignUp(d),
        finish: ({ d }) => close(d),
      },
      second_device_cart: {
        // A phone that just signed in to a new account with an empty cart.
        async prepare() {
          const address = email();
          const laptop = await open();
          await fillSignUp(laptop, address);
          await submitSignUp(laptop);
          await close(laptop);
          return { d: await signIn(address) };
        },
        async run({ d }) {
          await d.page.goto(`${origin}/cart`);
          await visible(d.page.getByText("Your cart is empty"), { timeout: 120_000 });
        },
        finish: ({ d }) => close(d),
      },
      return_visit: {
        async run() {
          await customer.page.goto(origin);
          await visible(product(customer.page));
        },
        async finish() {
          await customer.page.goto(`${origin}/cart`);
          await visible(quantity(customer.page));
        },
      },
      cart_reload: {
        async run() {
          await customer.page.reload();
          await visible(quantity(customer.page));
        },
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
