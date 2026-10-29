// PosterShop (Next.js + Better Auth): sign-up into the editor, editor reload,
// a single-use invite link to a poster, invitee reload, live colour change.
import { clickUntil, device, poll, random, rateLimitGate, visible } from "../harness.mjs";

const canvas = (page) => page.getByRole("application", { name: "Poster canvas" });
const ellipse = (page) => canvas(page).locator('[data-kind="ellipse"]').nth(1);
const shape = (page, id) => canvas(page).locator(`[data-shape-id="${id}"]`);

export default function posterShop({ server, browser }) {
  const { origin } = server;
  const gate = rateLimitGate();
  const devices = new Set();
  const open = async () => {
    const d = await device(browser);
    devices.add(d);
    return d;
  };
  const close = async (d) => {
    devices.delete(d);
    await d?.close();
  };
  const fillSignUp = async ({ page }, name) => {
    await page.goto(origin);
    await visible(page.getByRole("button", { name: "Create an account" }), { timeout: 240_000 });
    await clickUntil(
      page.getByRole("button", { name: "Create an account" }),
      page.getByLabel("Name"),
    );
    await page.getByLabel("Name").fill(name);
    await page.getByLabel("Email").fill(`${name.toLowerCase()}-${random()}@poster.test`);
    await page.getByLabel("Password").fill("poster-shop-demo");
    await gate();
  };
  const submitSignUp = async ({ page }) => {
    await page.getByRole("button", { name: "Create account" }).click();
    await visible(ellipse(page), { timeout: 120_000 });
  };
  const signUp = async (name) => {
    const d = await open();
    await fillSignUp(d, name);
    await submitSignUp(d);
    return d;
  };
  let owner, invitee, sun;

  return {
    async setup() {
      owner = await signUp("Ada");
      sun = await ellipse(owner.page).getAttribute("data-shape-id");
    },
    scenarios: {
      cold_signup: {
        async prepare() {
          const d = await open();
          await fillSignUp(d, "Grace");
          return { d };
        },
        run: ({ d }) => submitSignUp(d),
        finish: ({ d }) => close(d),
      },
      reload: {
        async run() {
          await owner.page.reload();
          await visible(shape(owner.page, sun));
        },
      },
      invite: {
        // A new user on their own poster; a new single-use invite link from
        // the owner (the dialog's default).
        async prepare() {
          const d = await signUp("Grace");
          const { page } = owner;
          await page.getByRole("button", { name: "Invite" }).click();
          await page.getByRole("button", { name: "Create invite link" }).click();
          const box = page.getByLabel("Invite link");
          await poll(async () => (await box.inputValue()).startsWith("http"), {
            what: "the invite link",
          });
          const link = await box.inputValue();
          await page.keyboard.press("Escape");
          // The link differs from the member's page only by its #fragment;
          // load it from a blank page, as a fresh navigation.
          await d.page.goto("about:blank");
          return { d, link };
        },
        async run({ d, link }) {
          await d.page.goto(link);
          await visible(shape(d.page, sun), { timeout: 90_000 });
        },
        // The last member to join is the invitee of the cases below.
        async finish({ d }) {
          await close(invitee);
          invitee = d;
        },
      },
      invitee_reload: {
        async run() {
          await invitee.page.reload();
          await visible(shape(invitee.page, sun));
        },
      },
      live_edit: {
        async prepare() {
          const before = await shape(invitee.page, sun).getAttribute("fill");
          await shape(owner.page, sun).click();
          const swatch = (name) =>
            owner.page
              .getByRole("radio", { name, exact: true })
              .or(owner.page.getByRole("button", { name, exact: true }))
              .first();
          await swatch("Teal").waitFor({ timeout: 10_000 });
          return {
            before,
            swatch: swatch(before?.toLowerCase().includes("teal") ? "Pink" : "Teal"),
          };
        },
        async run({ before, swatch }) {
          await swatch.click();
          await poll(async () => (await shape(invitee.page, sun).getAttribute("fill")) !== before, {
            what: "the new colour",
          });
        },
        finish: () => owner.page.keyboard.press("Escape"),
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
