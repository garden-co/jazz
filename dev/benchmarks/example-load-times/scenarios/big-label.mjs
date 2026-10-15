// BigLabel (Next.js + Better Auth): sign-up, releases reload, an admin adds a
// viewer by email and the viewer picks the label, viewer reload.
import { clickUntil, device, random, rateLimitGate, visible } from "../harness.mjs";

const LABEL = "Low Tide Records";
const nav = (page, name) => page.getByRole("link", { name, exact: true }).first();
const popover = (page) => page.locator("[popover]:popover-open");
const releaseRow = (page) => page.locator("table tbody a");

export default function bigLabel({ server, browser }) {
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
  const fillSignUp = async ({ page }, name, email) => {
    await page.goto(origin);
    const create = page.getByRole("button", { name: "Create an account instead" });
    await visible(create, { timeout: 180_000 });
    await clickUntil(create, page.getByLabel("Your name"));
    await page.getByLabel("Your name").fill(name);
    await page.getByLabel("Email").fill(email);
    await page.getByLabel("Password").fill("big-label-demo");
    await gate();
  };
  const submitSignUp = async ({ page }) => {
    await page.getByRole("button", { name: "Create account" }).click();
    await visible(nav(page, "Overview"), { timeout: 120_000 });
  };
  const signUp = async (name, email = `${name.toLowerCase()}-${random()}@biglabel.test`) => {
    const d = await open();
    await fillSignUp(d, name, email);
    await submitSignUp(d);
    return d;
  };
  let owner, invitee;

  return {
    async setup() {
      owner = await signUp("Ada");
      const { page } = owner;
      // The small demo data set, then its Low Tide Records label.
      await nav(page, "Settings").click();
      await page.getByText("Small", { exact: true }).first().click();
      await page.getByRole("button", { name: "Load demo data" }).click();
      await visible(page.getByText(/Loaded \d+ demo labels|already loaded/i), { timeout: 120_000 });
      const menu = page.locator("nav button[aria-label]").first();
      if (!(await popover(page).count())) await menu.click({ force: true });
      await visible(popover(page).getByText(LABEL, { exact: true }), { timeout: 30_000 });
      await popover(page).getByText(LABEL, { exact: true }).first().click();
      if (await popover(page).count()) await page.keyboard.press("Escape");
      await nav(page, "Releases").click();
      await visible(releaseRow(page));
    },
    scenarios: {
      cold_signup: {
        async prepare() {
          const d = await open();
          await fillSignUp(d, "Bo", `bo-${random()}@biglabel.test`);
          return { d };
        },
        run: ({ d }) => submitSignUp(d),
        finish: ({ d }) => close(d),
      },
      reload: {
        async run() {
          await owner.page.reload();
          await visible(releaseRow(owner.page));
        },
      },
      invite_pick_label: {
        // A new user; the admin adds them as a viewer by email; the label
        // shows up in their open label menu.
        async prepare() {
          const email = `bo-${random()}@biglabel.test`;
          const d = await signUp("Bo", email);
          const { page } = owner;
          await nav(page, "People").click();
          await page.getByRole("button", { name: "Add member" }).click();
          const dialog = page.getByRole("dialog");
          await dialog.getByLabel("Email").fill(email);
          await dialog.getByRole("combobox", { name: "Role" }).click();
          await page.getByRole("option", { name: "Viewer" }).first().click();
          await d.page.locator("nav button[aria-label]").first().click({ force: true });
          await visible(popover(d.page));
          await dialog.getByRole("button", { name: "Add member" }).click();
          await visible(popover(d.page).getByText(LABEL, { exact: true }));
          await nav(page, "Releases").click();
          await visible(releaseRow(page));
          return { d };
        },
        async run({ d }) {
          await popover(d.page).getByText(LABEL, { exact: true }).first().click();
          // The viewer's Releases link is not clickable here; the hash route
          // is a client-side navigation, as in the original harness.
          await d.page.evaluate(() => {
            location.hash = "#/releases";
          });
          await visible(releaseRow(d.page));
        },
        // The last viewer is the invitee of the case below.
        async finish({ d }) {
          await close(invitee);
          invitee = d;
        },
      },
      invitee_reload: {
        async run() {
          await invitee.page.reload();
          await visible(releaseRow(invitee.page));
        },
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
