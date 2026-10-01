// Examples-page walkthrough for Wequencer (examples/wequencer, Next.js + Better Auth).
//   node scripts/example-videos/wequencer.mjs   # writes public/examples/videos/wequencer.*
import { nextServer, record } from "./walkthrough.mjs";
import { click, pointAt, sleep, type } from "./stage.mjs";

const port = 3463;
const origin = `http://127.0.0.1:${port}`;
const uniq = Date.now() % 100000;

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

await record({
  id: "wequencer",
  app: "examples/wequencer/apps/next-betterauth",
  server: ({ dir }) => nextServer({ dir, port }),
  async run(stage) {
    const device = { address: "wequencer.example.com" };
    const ada = await stage.device("a", { ...device, name: "Ada's laptop" });
    const ben = await stage.device("b", { ...device, name: "Ben's laptop", color: "#ea580c" });
    for (const page of [ada, ben]) page.setDefaultTimeout(90_000);
    const pad = (page, name) => page.getByRole("button", { name, exact: true });
    const pressed = (page, name) => pad(page, name).getAttribute("aria-pressed");
    const toggle = async (page, name) =>
      click(page, pad(page, name), { steps: 4, hover: 120, after: 150 });
    const signUp = async (page, name, slow) => {
      await (slow ? click : (p, l) => l.click())(
        page,
        page.getByRole("button", { name: "Create an account" }),
      );
      const fill = async (label, text) =>
        slow
          ? type(page, page.getByLabel(label), text, { delay: 35 })
          : page.getByLabel(label).fill(text);
      await fill("Name", name);
      await fill("Email", `${name.toLowerCase()}${uniq}@example.com`);
      await page.getByLabel("Password").fill("wequencer-demo-2026");
      await (slow ? click : (p, l) => l.click())(
        page,
        page.getByRole("button", { name: "Create account" }),
      );
      await page.waitForURL("**/dashboard", { timeout: 120_000 });
      await page.getByTestId("member-id").waitFor({ timeout: 120_000 });
    };

    // Off camera: compile the dashboard route, and Ben signs up.
    await ben.goto(origin);
    await ben.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 240_000 });
    await signUp(ben, "Ben", false);
    const benId = (await ben.getByTestId("member-id").textContent()).trim();
    await ada.goto(origin);
    await ada.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 120_000 });

    await stage.start();
    await stage.split("a", "b", { scale: 0.8 });
    stage.roll();
    await stage.title(
      "Wequencer",
      "A shared step sequencer: one pattern, one transport, one mix. Next.js + Better Auth + Jazz.",
      2600,
    );
    await stage.caption("Ada signs up. The server bootstraps her account in one transaction.");
    await signUp(ada, "Ada", true);
    await stage.caption("");

    await click(ada, ada.getByRole("button", { name: "New session" }).first());
    await type(ada, ada.getByLabel("Title"), "Friday jam", { clear: true, delay: 45 });
    await click(ada, ada.getByRole("button", { name: "Create session" }));
    await ada.getByRole("heading", { name: "Friday jam" }).waitFor({ timeout: 120_000 });
    await sleep(1200);

    await stage.caption("Ada adds Ben as an editor by his account ID");
    await click(ada, ada.getByRole("button", { name: "Members" }));
    const dialog = ada.getByRole("dialog");
    await pointAt(ada, dialog.getByLabel("Collaborator account ID"));
    await dialog.getByLabel("Collaborator account ID").fill(benId);
    await click(ada, dialog.getByRole("button", { name: "Add collaborator" }), { after: 1200 });
    await ada.keyboard.press("Escape");

    // Ben's session list is a live query of the sessions he can see.
    const link = ben.getByRole("link", { name: "Friday jam" });
    await link.waitFor({ timeout: 30_000 });
    await stage.caption(
      "Ben's session list is a live query: “Friday jam” appears as he's added",
      2600,
    );
    await click(ben, ben.getByRole("heading", { name: "Friday jam" }), { after: 300 });
    await ben.waitForURL(/\/dashboard\/[0-9a-f-]+/, { timeout: 60_000 });
    await ben.getByRole("button", { name: "Members" }).waitFor({ timeout: 120_000 });
    await sleep(800);
    await stage.caption("");

    await stage.caption("Both program the same pattern, live");
    for (const step of [2, 10, 12, 13]) await toggle(ben, `Snare, step ${step}`);
    for (const step of [3, 7, 11]) await toggle(ada, `Open hat, step ${step}`);
    // Both views converge on the same pattern.
    await until(
      async () =>
        (await pressed(ada, "Snare, step 13")) === (await pressed(ben, "Snare, step 13")) &&
        (await pressed(ben, "Open hat, step 11")) === (await pressed(ada, "Open hat, step 11")),
    );
    stage.poster();
    await sleep(1200);

    await stage.caption("One shared transport: Ben presses Play, and Ada's sequencer plays too");
    await click(ben, ben.getByRole("button", { name: "Play" }));
    await ada.getByRole("button", { name: "Stop" }).waitFor({ timeout: 30_000 });
    await sleep(2500);

    await stage.caption("Ada sets the tempo to 150 for everyone");
    const tempo = ada.getByRole("spinbutton", { name: "Tempo" });
    await click(ada, tempo, { after: 100 });
    await tempo.fill("150");
    await tempo.press("Tab");
    await until(
      async () => (await ben.getByRole("spinbutton", { name: "Tempo" }).inputValue()) === "150",
    );
    await sleep(1800);

    // Wequencer waits for the server to confirm each pad edit, so the one who
    // goes offline here is Ben, while Ada keeps editing.
    await stage.caption("Ben's Wi-Fi drops…");
    await stage.wifi("b", false);
    const kick = ["Kick, step 4", "Kick, step 8", "Kick, step 14"];
    const before = await Promise.all(kick.map((name) => pressed(ben, name)));
    await stage.caption("…while Ada programs the kick");
    for (const name of kick) await toggle(ada, name);
    await sleep(1500);
    if ((await pressed(ben, kick[2])) !== before[2])
      throw new Error("Ada's edit reached Ben while his Wi-Fi was off");
    await stage.caption("Ben's pattern doesn't have those steps yet", 1800);
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("b", true);
    if (
      !(await until(
        async () => (await pressed(ben, kick[2])) === (await pressed(ada, kick[2])),
        30_000,
      ))
    )
      throw new Error("Ada's steps never reached Ben");
    await stage.caption("…and Ada's kick arrives in Ben's pattern", 2400);

    await stage.caption("Ada adds a second pattern; it shows up for Ben");
    await click(ada, ada.getByRole("button", { name: "Add pattern" }));
    await ben.getByText("Pattern 2", { exact: true }).first().waitFor({ timeout: 30_000 });
    await sleep(1800);

    await stage.caption("Ada reloads: pattern, mix and transport are all still there");
    await ada.reload();
    await stage.recast("a");
    await ada.getByRole("heading", { name: "Friday jam" }).waitFor({ timeout: 120_000 });
    await sleep(2500);

    await stage.caption("Dark mode");
    for (const page of [ada, ben]) await page.emulateMedia({ colorScheme: "dark" });
    await sleep(2500);
    await click(ada, ada.getByRole("button", { name: "Stop" })).catch(() => {});
    await stage.caption("");
  },
});
