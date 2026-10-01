// Examples-page walkthrough for StagePlan (examples/stage-plan, React + Vite).
//   node scripts/example-videos/stage-plan.mjs   # writes public/examples/videos/stage-plan.*
import { rm } from "node:fs/promises";
import { join } from "node:path";
import { record } from "./walkthrough.mjs";
import { click, pointAt, sleep, type } from "./stage.mjs";

const SHOW = "The Late Lanterns: album launch";
const port = 5392;
const url = `http://127.0.0.1:${port}/`;

await record({
  id: "stage-plan",
  app: "examples/stage-plan/apps/react-localfirst",
  server: ({ dir }) => ({
    before: () =>
      rm(join(dir, "node_modules/.cache/jazz-dev-server"), { recursive: true, force: true }),
    command: "pnpm",
    args: ["exec", "vite", "--port", String(port), "--strictPort", "--host", "127.0.0.1"],
    ready: /Local:\s+http/,
  }),
  async run(stage) {
    const device = { address: "stageplan.example.com", keepPorts: [port] };
    const a = await stage.device("a", { ...device, name: "Mia's laptop", color: "#2563eb" });
    const b = await stage.device("b", { ...device, name: "Cole's laptop", color: "#ea580c" });
    const column = (page, status) => page.locator(`[data-status="${status}"]`);
    const card = (page, title, status) =>
      (status ? column(page, status) : page).locator("[data-task-id]", { hasText: title });
    const rename = async (page, name) => {
      await page.getByRole("button", { name: /^Stagehand / }).click();
      await page.getByRole("textbox", { name: "Name" }).fill(name);
      await page.getByRole("button", { name: "Save" }).click();
      await page.getByRole("button", { name }).waitFor();
    };
    const move = async (page, title, from, key) => {
      const c = card(page, title, from);
      await pointAt(page, c);
      await c.locator("a").first().focus();
      await page.keyboard.press(key);
    };
    const add = async (page, title) => {
      await type(page, page.getByRole("textbox", { name: "New task" }), title);
      await click(page, page.getByRole("button", { name: "Add task" }), { after: 300 });
    };

    await a.goto(url);
    await a.getByRole("link", { name: SHOW }).waitFor({ timeout: 90_000 });
    await rename(a, "Mia");

    await stage.start();
    await stage.full("a");
    stage.roll();
    await stage.title(
      "StagePlan",
      "A crew prepares shows on a live board per show. React + Jazz, local-first accounts.",
      2600,
    );
    await stage.caption(
      "No sign-up: the browser holds a local-first account, and the demo show is already here",
      3000,
    );
    await stage.caption("");
    await click(a, a.getByRole("heading", { name: SHOW }), { after: 300 });
    await card(a, "Soundcheck", "todo").waitFor({ timeout: 30_000 });
    await sleep(1000);

    await add(a, "Tape the set list to the floor");
    await stage.caption("Writes apply instantly, locally first, and sync in the background", 1500);
    await move(a, "Soundcheck", "todo", "ArrowRight");
    await card(a, "Soundcheck", "doing").waitFor();
    await sleep(1200);
    await stage.caption("");

    // Task detail: comments and activity.
    await click(a, card(a, "Soundcheck", "doing"), { after: 900 });
    const box = a.getByPlaceholder("Add a comment for the crew");
    await type(a, box, "Band arrives at five. Drums first.");
    await click(a, a.getByRole("button", { name: "Comment" }), { after: 1200 });
    await a.mouse.wheel(0, 500);
    await sleep(1400);
    await a.keyboard.press("Escape");
    await sleep(600);

    // Invite link.
    await click(a, a.getByRole("link", { name: /^Crew \(/ }), { after: 800 });
    const invite = a.getByRole("textbox", { name: "Invite link" });
    await invite.and(a.locator(":not([value=''])")).waitFor();
    const inviteLink = await invite.inputValue();
    await pointAt(a, a.getByRole("button", { name: /Copy link/ }));
    await stage.caption("Only the crew chief can read the invite code", 2400);
    await stage.caption("");

    // Cole, on another laptop, opens the link (off camera until it lands).
    await b.goto(inviteLink);
    await b.getByRole("button", { name: "Add task" }).waitFor({ timeout: 60_000 });
    await rename(b, "Cole");
    await stage.split("a", "b", { scale: 0.64 });
    await a.getByText("Cole").first().waitFor({ timeout: 30_000 });
    await stage.caption(
      "Cole joined through the invite link, and Mia's crew list updated live",
      2800,
    );
    await stage.caption("");
    await click(a, a.getByRole("link", { name: "Board" }), { after: 800 });

    await move(b, "Line check", "doing", "ArrowRight");
    await card(a, "Line check", "done").waitFor({ timeout: 20_000 });
    await stage.caption("Cole moves “Line check” to Done, and it moves on Mia's board too", 2600);

    // Live query: Mia's open task shows just that task's comments, live.
    await stage.caption("Mia opens soundcheck: a live query for just its comments");
    await click(a, card(a, "Soundcheck"), { after: 300 });
    await a.getByPlaceholder("Add a comment for the crew").waitFor();
    await click(b, card(b, "Soundcheck"), { after: 300 });
    await stage.caption("Cole adds a comment to that task…");
    await type(b, b.getByPlaceholder("Add a comment for the crew"), "Kick drum mic is live");
    await click(b, b.getByRole("button", { name: "Comment" }), { after: 200 });
    await a.getByText("Kick drum mic is live").waitFor();
    stage.poster();
    await stage.caption("…and it appears in Mia's open task immediately", 2400);
    for (const page of [b, a]) {
      await page.keyboard.press("Escape");
      await page.getByPlaceholder("Add a comment for the crew").waitFor({ state: "hidden" });
    }

    // Wi-Fi off: local edits, then catch-up.
    await stage.caption("Mia turns off her laptop's Wi-Fi…");
    await stage.wifi("a", false);
    await stage.caption("…and keeps working: her edits apply locally");
    await add(a, "Check the fire exits");
    await move(a, "Hazer", "blocked", "ArrowLeft");
    await sleep(1200);
    await stage.caption("Cole's board doesn't have them yet", 2200);
    if (await card(b, "Check the fire exits").count())
      throw new Error("An offline edit reached Cole while Mia's Wi-Fi was off");
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("a", true);
    await card(b, "Check the fire exits", "todo").waitFor({ timeout: 30_000 });
    await stage.caption("…and Cole's board catches up", 2400);
    await stage.caption("");

    // The private checklist, with a live filter.
    await stage.full("a");
    await click(a, a.getByRole("link", { name: "Checklist" }), { after: 800 });
    await stage.caption("A private checklist for show day. Only its owner can read it.");
    const item = a.getByPlaceholder(/Spare gaffer tape/);
    for (const title of ["In-ears", "Spare batteries", "Spare gaffer tape"]) {
      await type(a, item, title, { delay: 35 });
      await a.keyboard.press("Enter");
      await sleep(250);
    }
    await type(a, a.getByPlaceholder("Filter as you type"), "Spare");
    await stage.caption("The filter is part of the query…", 1600);
    await type(a, item, "Spare strings", { delay: 45 });
    await a.keyboard.press("Enter");
    await a.locator("#checklist").getByText("Spare strings").waitFor();
    await stage.caption("…so a new matching item shows up in the filtered list right away", 2800);
    await stage.caption("");
  },
});
