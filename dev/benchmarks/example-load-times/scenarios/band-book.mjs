// BandBook (Next.js + Better Auth): sign-up (the server seeds a band), page
// reload, edit link to a page, invitee reload, live text edit.
import { clickUntil, device, poll, random, rateLimitGate, visible } from "../harness.mjs";

const side = (page, name) => page.getByRole("button", { name, exact: true }).first();
const title = (page) => page.getByRole("textbox", { name: "Page title" });
// Block text is a rich text editor per block.
const blocks = (page) => page.locator(".bb-rich-text [contenteditable]");
const hasText = (page, needle) =>
  blocks(page).evaluateAll((els, n) => els.some((e) => e.textContent.includes(n)), needle);
const pageText = (page) => poll(() => hasText(page, "Key of D"), { what: '"Key of D"' });

export default function bandBook({ server, browser }) {
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
    await visible(page.getByRole("button", { name: "Create an account" }), { timeout: 180_000 });
    await clickUntil(
      page.getByRole("button", { name: "Create an account" }),
      page.getByLabel("Name"),
    );
    await page.getByLabel("Name").fill(name);
    await page.getByLabel("Email").fill(`${name.toLowerCase()}-${random()}@band-book.test`);
    await page.getByLabel("Password").fill("correct horse battery");
    await gate();
  };
  const submitSignUp = async ({ page }) => {
    await page.getByRole("button", { name: "Create account" }).click();
    await page.waitForURL(/\/workspace/, { timeout: 120_000 });
    await visible(side(page, "Setlist: spring tour"), { timeout: 120_000 });
  };
  const signUp = async (name) => {
    const d = await open();
    await fillSignUp(d, name);
    await submitSignUp(d);
    return d;
  };

  let owner, invitee, inviteUrl;
  return {
    async setup() {
      owner = await signUp("Ada");
      const { page } = owner;
      await side(page, "Songs").click();
      await side(page, "Harbour lights").click();
      await title(page).waitFor();
      await pageText(page);
      await page.getByRole("button", { name: "Share" }).click();
      await page.getByRole("button", { name: "Create link" }).click();
      const link = page.getByRole("dialog").getByRole("textbox").last();
      await poll(async () => /\/invite\//.test(await link.inputValue()), { what: "the edit link" });
      inviteUrl = await link.inputValue();
      await page.getByRole("button", { name: "Done" }).click();
    },
    scenarios: {
      cold_signup: {
        async prepare() {
          const d = await open();
          await fillSignUp(d, "Bo");
          return { d };
        },
        run: ({ d }) => submitSignUp(d),
        finish: ({ d }) => close(d),
      },
      reload: {
        async run() {
          await owner.page.reload();
          await visible(title(owner.page));
          await pageText(owner.page);
        },
      },
      invite: {
        prepare: async () => ({ d: await signUp("Bo") }),
        async run({ d }) {
          await d.page.goto(inviteUrl);
          await d.page.waitForURL(/p=/, { timeout: 60_000 });
          await visible(title(d.page));
          await pageText(d.page);
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
          await pageText(invitee.page);
        },
      },
      live_edit: {
        async prepare() {
          const { page } = owner;
          const i = await blocks(page).evaluateAll((els) =>
            els.findIndex((e) => e.textContent.includes("Key of D")),
          );
          await blocks(page).nth(i).click();
          await page.keyboard.press("End");
          return { needle: `Count ${random()}` };
        },
        async run({ needle }) {
          await owner.page.keyboard.insertText(` ${needle}`);
          await poll(() => hasText(invitee.page, needle), { what: needle });
        },
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
