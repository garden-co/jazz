// World Tour (Vue + Vite, local-first account): the first visitor of a fresh
// sync server claims the demo band. First visit, reload, public link, invite
// link and join, invitee reload, live rename.
import { device, mark, poll, random, visible } from "../harness.mjs";

const pins = (page) => page.locator(".stop-pin");
const stop = (page, n) => pins(page).nth(n - 1);
const attached = { state: "attached" };

export default function worldTour({ server, browser }) {
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
  /** The first visit after a fresh sync server: all 12 demo stops. */
  const claim = async (page) => {
    await page.goto(url);
    await visible(page.getByText("Your band."), { timeout: 90_000 });
    await visible(stop(page, 12), { ...attached, timeout: 60_000 });
  };
  let owner, invitee, stops, inviteLink, publicLink;
  // The owner, links and invitee belong to one sync server; a cold visit
  // replaces it, so they are made again on first use after one.
  const ensureOwner = async () => {
    if (owner) return;
    await server.restartSync();
    owner = await open();
    const { page } = owner;
    await claim(page);
    stops = await pins(page).count();
    await page.getByRole("button", { name: "Band", exact: true }).click();
    const box = page.getByLabel("Invite link");
    await poll(async () => (await box.inputValue()).startsWith("http"), {
      what: "the invite link",
    });
    inviteLink = await box.inputValue();
    publicLink = inviteLink.replace(/\/join\/.*$/, "");
    await page
      .getByRole("button", { name: "Close" })
      .first()
      .click()
      .catch(() => {});
  };
  const allStops = (page) => visible(stop(page, stops), attached);
  const joinDialog = (page) => page.getByRole("dialog", { name: /^Join / });

  return {
    scenarios: {
      cold: {
        async prepare() {
          await close(owner);
          await close(invitee);
          owner = invitee = undefined;
          await server.restartSync();
          return { d: await open() };
        },
        run: ({ d }) => claim(d.page),
        finish: ({ d }) => close(d),
      },
      reload: {
        prepare: ensureOwner,
        async run() {
          await owner.page.reload();
          await visible(owner.page.getByText("Your band."));
          await allStops(owner.page);
        },
      },
      public_link: {
        async prepare() {
          await ensureOwner();
          return { d: await open() };
        },
        async run({ d }) {
          await d.page.goto(publicLink);
          await visible(d.page.getByRole("dialog"));
        },
        finish: ({ d }) => close(d),
      },
      invite_prompt: {
        async prepare() {
          await ensureOwner();
          return { d: await open() };
        },
        async run({ d }) {
          await d.page.goto(inviteLink);
          await visible(joinDialog(d.page));
        },
        finish: ({ d }) => close(d),
      },
      invite_join: {
        async prepare() {
          await ensureOwner();
          const d = await open();
          await d.page.goto(inviteLink);
          await visible(joinDialog(d.page));
          await d.page.getByLabel("Your name").fill("Rosa");
          return { d };
        },
        async run({ d }) {
          await d.page.getByRole("button", { name: "Join band" }).click();
          mark("clicked Join band");
          await visible(d.page.getByText("You're in this band."));
          mark("member view");
          await allStops(d.page);
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
          await visible(invitee.page.getByText("You're in this band."));
          mark("member view");
          await allStops(invitee.page);
        },
      },
      live_edit: {
        async prepare() {
          const name = `Tour ${random()}`;
          await owner.page.getByRole("button", { name: "Rename band" }).click();
          await owner.page.getByLabel("Band name").fill(name);
          return { name };
        },
        async run({ name }) {
          await owner.page.getByLabel("Band name").press("Enter");
          await visible(invitee.page.getByRole("heading", { name }));
        },
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}
