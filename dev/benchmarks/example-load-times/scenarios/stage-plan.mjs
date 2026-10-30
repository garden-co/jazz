// StagePlan (React + Vite, local-first account): first visit, board reload,
// invite link, invitee reload, live task.
import { device, random, visible } from "../harness.mjs";

const SHOW = "The Late Lanterns: album launch";
const CARD = "Soundcheck with the band";
const card = (page, title = CARD) => page.locator("[data-task-id]", { hasText: title });
const showLink = (page) => page.getByRole("link", { name: SHOW });

export default function stagePlan({ server, browser }) {
  const url = `${server.origin}/`;
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
  let chief, invitee, board, inviteLink;

  return {
    // A new visitor gets a local-first account and the demo show; they are
    // its crew chief.
    async setup() {
      chief = await open();
      const { page } = chief;
      await page.goto(url);
      await visible(showLink(page), { timeout: 90_000 });
      board = url + (await showLink(page).first().getAttribute("href"));
      await page.goto(`${board}/crew`);
      const invite = page.getByRole("textbox", { name: "Invite link" });
      await invite.and(page.locator(":not([value=''])")).waitFor();
      inviteLink = await invite.inputValue();
      await page.goto(board);
      await visible(card(page));
    },
    scenarios: {
      cold: {
        prepare: async () => ({ d: await open() }),
        async run({ d }) {
          await d.page.goto(url);
          await visible(showLink(d.page), { timeout: 90_000 });
        },
        finish: ({ d }) => close(d),
      },
      reload: {
        async run() {
          await chief.page.reload();
          await visible(card(chief.page));
        },
      },
      invite: {
        prepare: async () => ({ d: await open() }),
        async run({ d }) {
          await d.page.goto(inviteLink);
          await visible(card(d.page));
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
          await visible(card(invitee.page));
        },
      },
      live_edit: {
        async prepare() {
          const title = `Gaffer run ${random()}`;
          await chief.page.getByRole("textbox", { name: "New task" }).fill(title);
          return { title };
        },
        async run({ title }) {
          await chief.page.getByRole("button", { name: "Add task" }).click();
          await visible(card(invitee.page, title));
        },
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
