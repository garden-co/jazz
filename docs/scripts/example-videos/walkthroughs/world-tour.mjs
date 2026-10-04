// World Tour (examples/world-tour, Vue + Vite): server and actions for
// world-tour.storyboard.ts. `state.tentative` holds the stops the public page
// hides at first, so captions can name them ("{tentative.0}").
import { click as stageClick, sleep, type as stageType } from "../stage.mjs";

// The globe animates every frame on a software renderer, so point quickly and
// click directly.
const fast = { steps: 2, hover: 150, direct: true };
const click = (page, locator, options) => stageClick(page, locator, { ...fast, ...options });
const type = (page, locator, text, options) =>
  stageType(page, locator, text, { ...fast, ...options });

const port = 5391;
const url = `http://127.0.0.1:${port}/`;

export const app = "examples/world-tour";
export const server = () => ({
  command: "pnpm",
  args: ["exec", "vite", "dev", "--port", String(port), "--strictPort", "--host", "127.0.0.1"],
  // A fresh in-memory server (so the demo tour is new) and no dev inspector toggle.
  env: { VITE_E2E: "true" },
  ready: /Local:\s+http/,
});
export const deviceOptions = { keepPorts: [port] };

const pin = (page, name) => page.locator(`.stop-pin[aria-label="${name}"]`);
const pinNames = (page) =>
  page.locator(".stop-pin").evaluateAll((els) => els.map((e) => e.getAttribute("aria-label")));

export const actions = {
  async openDemoTour({ page }) {
    await page.goto(url);
    await page.getByText("Your band.").waitFor({ timeout: 90_000 });
    await page.locator(".stop-pin").nth(11).waitFor({ state: "attached", timeout: 60_000 });
    await sleep(2000);
  },
  async playTour({ page }) {
    await click(page, page.getByRole("button", { name: "Play tour" }), { after: 3000 });
    await click(page, page.getByRole("button", { name: "Stop tour" }), { after: 600 });
  },
  /** Opens the first stop's detail; keeps the names of every stop the owner sees. */
  async openFirstStop({ page, state }) {
    state.ownerPins = await pinNames(page);
    await page.locator(".stop-pin").first().dispatchEvent("click");
    await page.locator(".stop-detail").waitFor();
  },
  async close({ page }, { after = 400 } = {}) {
    await click(page, page.getByRole("button", { name: "Close" }), { after });
  },
  async closeIfOpen({ page }) {
    await page
      .getByRole("button", { name: "Close" })
      .first()
      .click()
      .catch(() => {});
  },
  async openBand({ page }, { after = 900 } = {}) {
    await click(page, page.getByRole("button", { name: "Band", exact: true }), { after });
  },
  /** Opens the band panel and keeps its invite link and the public link. */
  async readBandLinks(ctx) {
    await actions.openBand(ctx);
    ctx.state.inviteLink = await ctx.page.getByLabel("Invite link").inputValue();
    ctx.state.publicLink = ctx.state.inviteLink.replace(/\/join\/.*$/, "");
  },
  async openPublicLink({ stage, on, page, state }) {
    await page.goto(state.publicLink);
    await stage.recast(on);
    await page.getByRole("dialog").waitFor({ timeout: 60_000 });
  },
  async exploreGlobe({ page }, { after = 400 } = {}) {
    await click(page, page.getByRole("button", { name: "Explore the globe" }), { after });
  },
  /** The stops the owner sees but this public page doesn't: the tentative ones. */
  async findTentativeStops({ page, state }) {
    const visible = await pinNames(page);
    state.tentative = state.ownerPins.filter((n) => !visible.includes(n));
    if (state.tentative.length < 2)
      throw new Error(`Expected two tentative stops, got ${state.tentative.length}`);
  },
  async confirmStop({ page, state }, i) {
    await pin(page, state.tentative[i]).dispatchEvent("click");
    await page.locator(".stop-detail").waitFor();
    await sleep(600);
    await click(page, page.getByRole("button", { name: "Edit stop" }));
    await page.locator(".stop-detail select").selectOption("confirmed");
    await sleep(500);
    await click(page, page.getByRole("button", { name: "Save" }));
  },
  async waitForStop({ page, state }, i) {
    await pin(page, state.tentative[i]).waitFor({ state: "attached", timeout: 30_000 });
  },
  async expectNoStop({ page, state }, i) {
    if (await pin(page, state.tentative[i]).count())
      throw new Error("An offline confirmation reached the fan while Wi-Fi was off");
  },
  async openStop({ page, state }, i) {
    await pin(page, state.tentative[i]).dispatchEvent("click");
  },
  async openInviteLink({ stage, on, page, state }) {
    await page.goto(state.inviteLink);
    await stage.recast(on);
    await page.getByRole("dialog", { name: /^Join / }).waitFor({ timeout: 60_000 });
  },
  async joinBand({ page }, name) {
    await type(page, page.getByLabel("Your name"), name);
    await click(page, page.getByRole("button", { name: "Join band" }));
    await page.getByText("You're in this band.").waitFor({ timeout: 30_000 });
  },
  async waitForMember({ page }, name) {
    await page.locator(".member-list li", { hasText: name }).waitFor({ timeout: 30_000 });
  },
  async waitForAllStops({ page, state }) {
    await page
      .locator(".stop-pin")
      .nth(state.ownerPins.length - 1)
      .waitFor({ state: "attached", timeout: 30_000 });
  },
  async blank({ page }) {
    await page.goto("about:blank");
  },
};
