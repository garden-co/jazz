// StagePlan actions shared by the examples-page walkthrough (stage-plan) and
// the homepage one (stage-plan-two-devices). Both run the React example on Vite.
import { click, pointAt, sleep, type } from "../stage.mjs";
import { viteServer } from "../walkthrough.mjs";

export const app = "examples/stage-plan/apps/react-localfirst";

const column = (page, status) => page.locator(`[data-status="${status}"]`);
const card = (page, title, status) =>
  (status ? column(page, status) : page).locator("[data-task-id]", { hasText: title });
const commentBox = (page) => page.getByPlaceholder("Add a comment for the crew");
const STATUSES = ["todo", "doing", "done", "blocked"];

/** The Vite server and the actions, for an example served on `port`. */
export function stagePlan(port) {
  const url = `http://127.0.0.1:${port}/`;
  const rename = async (page, name) => {
    await page.getByRole("button", { name: /^Stagehand / }).click();
    await page.getByRole("textbox", { name: "Name" }).fill(name);
    await page.getByRole("button", { name: "Save" }).click();
    await page.getByRole("button", { name }).waitFor();
  };

  const actions = {
    /** Off camera: the first visitor gets the demo show; their name is set. */
    async openDemoShow({ page, state }, show, name) {
      await page.goto(url);
      const link = page.getByRole("link", { name: show });
      await link.waitFor({ timeout: 90_000 });
      await rename(page, name);
      state.board = url + (await link.getAttribute("href"));
    },
    /** Off camera: reads the show's invite link from its crew page. */
    async readInviteLink({ page, state }) {
      await page.goto(`${state.board}/crew`);
      const invite = page.getByRole("textbox", { name: "Invite link" });
      await invite.and(page.locator(":not([value=''])")).waitFor();
      state.inviteLink = await invite.inputValue();
    },
    /** Opens the invite link on another device and sets the crew member's name. */
    async joinWithInvite({ page, state }, name) {
      await page.goto(state.inviteLink);
      await page.getByRole("button", { name: "Add task" }).waitFor({ timeout: 60_000 });
      await rename(page, name);
    },
    /** Off camera: back to the board, with the new crew member listed. */
    async reopenBoard({ page, state }, crew) {
      await page.goto(state.board);
      await page.getByRole("link", { name: `Crew (${crew})` }).waitFor();
    },
    async openShow({ page }, show) {
      await click(page, page.getByRole("heading", { name: show }), { after: 300 });
      await card(page, "Soundcheck", "todo").waitFor({ timeout: 30_000 });
    },
    async addTask({ page }, title, { after = 300 } = {}) {
      await type(page, page.getByRole("textbox", { name: "New task" }), title);
      await click(page, page.getByRole("button", { name: "Add task" }), { after });
    },
    /** Moves a card one column with the keyboard. */
    async moveCard({ page }, title, from, to) {
      const c = card(page, title, from);
      await pointAt(page, c);
      await c.locator("a").first().focus();
      await page.keyboard.press(
        STATUSES.indexOf(to) > STATUSES.indexOf(from) ? "ArrowRight" : "ArrowLeft",
      );
      await card(page, title, to).waitFor();
    },
    /** Drags a card into another column, the way a person would. */
    async dragCard({ page }, title, to) {
      await pointAt(page, card(page, title));
      await page.mouse.down();
      const box = await column(page, to).boundingBox();
      await page.mouse.move(box.x + box.width / 2, box.y + box.height - 24, { steps: 24 });
      await page.mouse.up();
      await card(page, title, to).waitFor();
    },
    async waitForCard({ page }, title, status) {
      await card(page, title, status).waitFor({ timeout: 30_000 });
    },
    async expectNoCard({ page }, title, status) {
      if (await card(page, title, status).count())
        throw new Error(`"${title}" reached this board while the other laptop was offline`);
    },
    async openCard({ page }, title, { after = 300 } = {}) {
      await click(page, card(page, title), { after });
      await commentBox(page).waitFor();
    },
    async comment({ page }, text, { after = 200 } = {}) {
      await type(page, commentBox(page), text);
      await click(page, page.getByRole("button", { name: "Comment" }), { after });
    },
    async closeCard({ page }) {
      await page.keyboard.press("Escape");
      await commentBox(page).waitFor({ state: "hidden" });
    },
    async scroll({ page }, dy) {
      await page.mouse.wheel(0, dy);
    },
    /** Opens the crew page and points at the invite link, which it keeps. */
    async showInviteLink({ page, state }) {
      await click(page, page.getByRole("link", { name: /^Crew \(/ }), { after: 800 });
      const invite = page.getByRole("textbox", { name: "Invite link" });
      await invite.and(page.locator(":not([value=''])")).waitFor();
      state.inviteLink = await invite.inputValue();
      await pointAt(page, page.getByRole("button", { name: /Copy link/ }));
    },
    async openTab({ page }, name) {
      await click(page, page.getByRole("link", { name }), { after: 800 });
    },
    async addChecklistItems({ page }, titles) {
      for (const title of titles) {
        await type(page, page.getByPlaceholder(/Spare gaffer tape/), title, { delay: 35 });
        await page.keyboard.press("Enter");
        await sleep(250);
      }
    },
    async filterChecklist({ page }, text) {
      await type(page, page.getByPlaceholder("Filter as you type"), text);
    },
    async addChecklistItem({ page }, title) {
      await type(page, page.getByPlaceholder(/Spare gaffer tape/), title, { delay: 45 });
      await page.keyboard.press("Enter");
      await page.locator("#checklist").getByText(title).waitFor();
    },
  };

  return {
    server: ({ dir }) => viteServer({ dir, port }),
    deviceOptions: { keepPorts: [port] },
    actions,
  };
}
