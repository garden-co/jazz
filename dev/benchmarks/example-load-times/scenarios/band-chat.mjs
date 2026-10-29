// BandChat (Next.js + Better Auth): sign-up, room reload, ask to join through
// a room link and get admitted, invitee reload, live message.
import { clickUntil, device, random, rateLimitGate, visible } from "../harness.mjs";

export default function bandChat({ server, browser }) {
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
    await page.getByLabel("Name").first().fill(name);
    await page.getByLabel("Email").first().fill(`${name.toLowerCase()}-${random()}@example.com`);
    await page.getByLabel("Password").first().fill("correct horse battery");
    await gate();
  };
  const submitSignUp = async ({ page }) => {
    await page.getByRole("button", { name: "Create account", exact: true }).click();
    await visible(page.getByText("Set up your profile"), { timeout: 120_000 });
  };
  /** A new account through profile setup, on its (empty) room list. */
  const signUp = async (name) => {
    const d = await open();
    await fillSignUp(d, name);
    await submitSignUp(d);
    await d.page.getByRole("button", { name: "Continue" }).click();
    await visible(d.page.getByText("No rooms yet"), { timeout: 120_000 });
    return d;
  };

  const room = `Rehearsal ${random()}`;
  const firstMessage = `Soundcheck at six ${random()}`;
  const heading = (page) => page.getByRole("heading", { name: room });
  const message = (page, text = firstMessage) => page.getByText(text);
  const ask = (page) => page.getByRole("button", { name: "Ask to join" });
  const inviteButton = (page) => page.getByRole("button", { name: /^Invite/ });
  let owner, invitee, roomId;

  /** A new user who opened the room link and sees the enabled join prompt. */
  const atPrompt = async () => {
    const d = await signUp("Gus");
    await d.page.goto(`${origin}/dashboard?join=${roomId}`);
    await ask(d.page).and(d.page.locator(":not([disabled]):not([aria-disabled='true'])")).waitFor();
    return d;
  };

  return {
    async setup() {
      owner = await signUp("Olive");
      const { page } = owner;
      await page.getByRole("button", { name: "Create a room" }).click();
      await page.getByLabel("Room name").fill(room);
      await page.getByRole("button", { name: "Create room" }).click();
      await heading(page).waitFor();
      roomId = new URL(page.url()).searchParams.get("room");
      await page.getByRole("textbox", { name: "Message input" }).fill(firstMessage);
      await page.keyboard.press("Enter");
      await visible(message(page));
    },
    scenarios: {
      cold_signup: {
        async prepare() {
          const d = await open();
          await fillSignUp(d, "Gus");
          return { d };
        },
        run: ({ d }) => submitSignUp(d),
        finish: ({ d }) => close(d),
      },
      reload: {
        async run() {
          await owner.page.reload();
          await visible(message(owner.page));
        },
      },
      invite_prompt: {
        prepare: async () => ({ d: await signUp("Gus") }),
        async run({ d }) {
          await d.page.goto(`${origin}/dashboard?join=${roomId}`);
          await visible(ask(d.page));
        },
        finish: ({ d }) => close(d),
      },
      invite_admit: {
        async prepare() {
          const d = await atPrompt();
          await ask(d.page).click();
          await visible(d.page.getByText("Waiting for the room creator"));
          await visible(inviteButton(owner.page).filter({ hasText: "1" }));
          await inviteButton(owner.page).first().click();
          await visible(owner.page.getByRole("button", { name: "Admit" }));
          return { d };
        },
        async run({ d }) {
          await owner.page.getByRole("button", { name: "Admit" }).first().click();
          await visible(message(d.page));
        },
        // The last member admitted is the invitee of the cases below.
        async finish({ d }) {
          await owner.page.keyboard.press("Escape");
          await close(invitee);
          invitee = d;
        },
      },
      invitee_reload: {
        async run() {
          await invitee.page.reload();
          await visible(message(invitee.page));
        },
      },
      live_message: {
        async prepare() {
          const text = `Live ${random()}`;
          await owner.page.getByRole("textbox", { name: "Message input" }).fill(text);
          return { text };
        },
        async run({ text }) {
          await owner.page.keyboard.press("Enter");
          await visible(message(invitee.page, text));
        },
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
