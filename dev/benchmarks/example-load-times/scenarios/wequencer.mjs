// Wequencer (Next.js + Better Auth): sign-up, session reload, a collaborator
// added by account ID opens the session, collaborator reload.
import { clickUntil, device, random, rateLimitGate, visible } from "../harness.mjs";

const PAD = "Kick, step 1";
const pad = (page, name = PAD) => page.getByRole("button", { name, exact: true });

export default function wequencer({ server, browser }) {
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
    await page.getByLabel("Email").fill(`${name.toLowerCase()}${random()}@example.com`);
    await page.getByLabel("Password").fill("wequencer-demo-2026");
    await gate();
  };
  const submitSignUp = async ({ page }) => {
    await page.getByRole("button", { name: "Create account" }).click();
    await page.waitForURL("**/dashboard", { timeout: 120_000 });
    await visible(page.getByRole("button", { name: "New session" }), { timeout: 120_000 });
  };
  const signUp = async (name) => {
    const d = await open();
    await fillSignUp(d, name);
    await submitSignUp(d);
    return d;
  };
  const title = `Friday jam ${random()}`;
  const heading = (page) => page.getByRole("heading", { name: title });
  let owner, invitee;

  return {
    async setup() {
      owner = await signUp("Ada");
      const { page } = owner;
      await page.getByRole("button", { name: "New session" }).first().click();
      await page.getByLabel("Title").fill(title);
      await page.getByRole("button", { name: "Create session" }).click();
      await visible(heading(page), { timeout: 120_000 });
      await visible(pad(page));
      await pad(page).click();
    },
    scenarios: {
      cold_signup: {
        async prepare() {
          const d = await open();
          await fillSignUp(d, "Ben");
          return { d };
        },
        run: ({ d }) => submitSignUp(d),
        finish: ({ d }) => close(d),
      },
      reload: {
        async run() {
          await owner.page.reload();
          await visible(pad(owner.page));
        },
      },
      invite_open: {
        // A new user, added by account ID; the session is listed on their
        // dashboard.
        async prepare() {
          const d = await signUp("Ben");
          await visible(d.page.getByTestId("member-id"));
          const id = (await d.page.getByTestId("member-id").first().textContent()).trim();
          const { page } = owner;
          await page.getByRole("button", { name: "Members" }).click();
          const dialog = page.getByRole("dialog");
          await dialog.getByLabel("Collaborator account ID").fill(id);
          await dialog.getByRole("button", { name: "Add collaborator" }).click();
          const link = d.page.getByRole("link", { name: title }).first();
          await visible(link);
          // The link has been seen to vanish and come back right after it
          // first shows; let it settle, as the original harness did.
          await d.page.waitForTimeout(3000);
          await page.keyboard.press("Escape");
          return { d, link, href: await link.getAttribute("href") };
        },
        async run({ d, link, href }) {
          // A Playwright click on the link times out (something intercepts
          // it); a DOM click still runs the client-side navigation.
          if (await link.isVisible()) await link.evaluate((el) => el.click());
          else await d.page.goto(new URL(href, origin).href);
          await visible(pad(d.page));
        },
        // The last collaborator is the invitee of the case below.
        async finish({ d }) {
          await close(invitee);
          invitee = d;
        },
      },
      invitee_reload: {
        async run() {
          await invitee.page.reload();
          await visible(pad(invitee.page));
        },
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
