// Wequencer (examples/wequencer, Next.js + Better Auth): server and actions for
// wequencer.storyboard.ts.
import { click, pointAt, sleep, type } from "../stage.mjs";
import { nextServer } from "../walkthrough.mjs";

const port = 3463;
const origin = `http://127.0.0.1:${port}`;
const uniq = Date.now() % 100000;

export const app = "examples/wequencer/apps/next-betterauth";
export const server = ({ dir }) => nextServer({ dir, port });

const pad = (page, name) => page.getByRole("button", { name, exact: true });
const pressed = (page, name) => pad(page, name).getAttribute("aria-pressed");

// Polls until `check` holds (pads and transport converge asynchronously).
async function until(check, ms = 20_000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    try {
      if (await check()) return true;
    } catch {}
    await sleep(250);
  }
  return false;
}

export const actions = {
  /** Off camera: a new account; its account ID is kept for invites. */
  async signUp({ on, page, state }, name) {
    await page.goto(origin);
    await page.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 240_000 });
    await page.getByRole("button", { name: "Create an account" }).click();
    await page.getByLabel("Name").fill(name);
    await page.getByLabel("Email").fill(`${name.toLowerCase()}${uniq}@example.com`);
    await page.getByLabel("Password").fill("wequencer-demo-2026");
    await page.getByRole("button", { name: "Create account" }).click();
    await page.waitForURL("**/dashboard", { timeout: 120_000 });
    await page.getByTestId("member-id").waitFor({ timeout: 120_000 });
    state[`${on}Id`] = (await page.getByTestId("member-id").textContent()).trim();
  },
  async createSession({ page }, title) {
    await click(page, page.getByRole("button", { name: "New session" }).first());
    await type(page, page.getByLabel("Title"), title, { clear: true, delay: 45 });
    await click(page, page.getByRole("button", { name: "Create session" }));
    await page.getByRole("heading", { name: title }).waitFor({ timeout: 120_000 });
  },
  /** Adds the `other` device's account as an editor. */
  async addCollaborator({ page, state }, other) {
    await click(page, page.getByRole("button", { name: "Members" }));
    const dialog = page.getByRole("dialog");
    await pointAt(page, dialog.getByLabel("Collaborator account ID"));
    await dialog.getByLabel("Collaborator account ID").fill(state[`${other}Id`]);
    await click(page, dialog.getByRole("button", { name: "Add collaborator" }), { after: 1200 });
    await page.keyboard.press("Escape");
  },
  async waitForSessionLink({ page }, title) {
    await page.getByRole("link", { name: title }).waitFor({ timeout: 30_000 });
  },
  async openSession({ page }, title) {
    await click(page, page.getByRole("heading", { name: title }), { after: 300 });
    await page.waitForURL(/\/dashboard\/[0-9a-f-]+/, { timeout: 60_000 });
    await page.getByRole("button", { name: "Members" }).waitFor({ timeout: 120_000 });
  },
  async toggle({ page }, names) {
    for (const name of names)
      await click(page, pad(page, name), { steps: 4, hover: 120, after: 150 });
  },
  /**
   * Waits until these pads match on this device and `other`. With `required`,
   * fails if they never do.
   */
  async waitForSamePads(
    { page, pages },
    other,
    names,
    { timeout = 20_000, required = false } = {},
  ) {
    const same = await until(async () => {
      for (const name of names)
        if ((await pressed(page, name)) !== (await pressed(pages[other], name))) return false;
      return true;
    }, timeout);
    if (required && !same) throw new Error(`${names.join(", ")} never matched`);
  },
  async rememberPads({ on, page, state }, names) {
    for (const name of names) state[`${on}:${name}`] = await pressed(page, name);
  },
  async expectPadsUnchanged({ on, page, state }, names) {
    for (const name of names)
      if ((await pressed(page, name)) !== state[`${on}:${name}`])
        throw new Error(`${name} changed on ${on} while its Wi-Fi was off`);
  },
  async play({ page }) {
    await click(page, page.getByRole("button", { name: "Play" }));
  },
  async waitForPlaying({ page }) {
    await page.getByRole("button", { name: "Stop" }).waitFor({ timeout: 30_000 });
  },
  async stop({ page }) {
    await click(page, page.getByRole("button", { name: "Stop" })).catch(() => {});
  },
  async setTempo({ page }, bpm) {
    const tempo = page.getByRole("spinbutton", { name: "Tempo" });
    await click(page, tempo, { after: 100 });
    await tempo.fill(String(bpm));
    await tempo.press("Tab");
  },
  async waitForTempo({ page }, bpm) {
    await until(
      async () =>
        (await page.getByRole("spinbutton", { name: "Tempo" }).inputValue()) === String(bpm),
    );
  },
  async addPattern({ page }) {
    await click(page, page.getByRole("button", { name: "Add pattern" }));
  },
  async reloadSession({ stage, on, page }, title) {
    await page.reload();
    await stage.recast(on);
    await page.getByRole("heading", { name: title }).waitFor({ timeout: 120_000 });
  },
};
