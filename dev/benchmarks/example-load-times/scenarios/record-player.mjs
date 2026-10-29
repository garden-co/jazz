// RecordPlayer (Next.js + Better Auth): sign-up into the library, library
// reload, a second user's first load of the shared catalogue.
import { device, random, rateLimitGate, visible } from "../harness.mjs";

const ALBUM = "Sine studies";

export default function recordPlayer({ server, browser }) {
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
  const fillSignUp = async ({ page }) => {
    await page.goto(origin);
    await visible(page.getByRole("heading", { name: "Sign in to RecordPlayer" }), {
      timeout: 180_000,
    });
    await page.getByLabel("Email").fill(`listener-${random()}@example.com`);
    await gate();
  };
  const submit = (page) => page.getByRole("button", { name: "Create account" }).click();
  const library = (page) => page.getByText("The library is empty").or(page.getByText(ALBUM));
  let listener;
  // The first listener adds the demo library, which every account then
  // shares. Created on first use, so the cold sign-ups before it see the
  // empty library a brand-new deployment shows.
  const ensureListener = async () => {
    if (listener) return;
    listener = await open();
    const { page } = listener;
    await fillSignUp(listener);
    await submit(page);
    await visible(library(page), { timeout: 120_000 });
    const add = page.getByRole("button", { name: "Add demo library" });
    if (await add.count()) await add.click();
    await visible(page.getByText(ALBUM), { timeout: 120_000 });
  };

  return {
    scenarios: {
      // The library view of a deployment nobody has added albums to yet.
      cold_signup: {
        async prepare() {
          const d = await open();
          await fillSignUp(d);
          return { d };
        },
        async run({ d }) {
          await submit(d.page);
          await visible(library(d.page), { timeout: 120_000 });
        },
        finish: ({ d }) => close(d),
      },
      reload: {
        prepare: ensureListener,
        async run() {
          await listener.page.reload();
          await visible(listener.page.getByText(ALBUM));
        },
      },
      second_user_signup: {
        async prepare() {
          await ensureListener();
          const d = await open();
          await fillSignUp(d);
          return { d };
        },
        async run({ d }) {
          await submit(d.page);
          await visible(d.page.getByText(ALBUM), { timeout: 120_000 });
        },
        finish: ({ d }) => close(d),
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
