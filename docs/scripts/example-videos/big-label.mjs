// Examples-page walkthrough for BigLabel (examples/big-label, Next.js + Better Auth).
//   node scripts/example-videos/big-label.mjs   # writes public/examples/videos/big-label.*
import { nextServer, record } from "./walkthrough.mjs";
import { click, pointAt, sleep, type } from "./stage.mjs";

const port = 3462;
const origin = `http://127.0.0.1:${port}`;
const run = Date.now().toString(36);
const adaEmail = `ada-${run}@biglabel.test`;
const boEmail = `bo-${run}@biglabel.test`;
const LABEL = "Low Tide Records";

await record({
  id: "big-label",
  app: "examples/big-label",
  server: ({ dir }) => nextServer({ dir, port }),
  async run(stage) {
    const device = { address: "biglabel.example.com" };
    const a = await stage.device("a", { ...device, name: "Ada's laptop" });
    const b = await stage.device("b", { ...device, name: "Bo's laptop", color: "#ea580c" });
    for (const page of [a, b]) page.setDefaultTimeout(90_000);
    const nav = (page, name) => page.getByRole("link", { name, exact: true }).first();
    const signUp = async (page, name, email) => {
      await page.getByRole("button", { name: "Create an account instead" }).click();
      await page.getByLabel("Your name").fill(name);
      await page.getByLabel("Email").fill(email);
      await page.getByLabel("Password").fill("big-label-demo");
      await page.getByRole("button", { name: "Create account" }).click();
      await nav(page, "Overview").waitFor({ timeout: 120_000 });
    };
    const openLabelMenu = async (page) => {
      const button = page.locator("nav button[aria-label]").first();
      const open = page.locator("[popover]:popover-open");
      await pointAt(page, button, { hover: 500 });
      if (!(await open.count())) await button.click({ force: true });
      await open.first().waitFor({ timeout: 10_000 });
      await sleep(500);
    };
    const switchLabel = async (page) => {
      await openLabelMenu(page);
      const item = page.locator("[popover]:popover-open").getByText(LABEL, { exact: true }).first();
      await click(page, item, { after: 1000 });
      if (await page.locator("[popover]:popover-open").count()) await page.keyboard.press("Escape");
    };
    const addArtist = async (name) => {
      await click(a, a.getByRole("button", { name: "Add artist" }).first(), { after: 500 });
      await type(a, a.getByRole("dialog").getByLabel("Name"), name);
      await click(
        a,
        a
          .getByRole("dialog")
          .getByRole("button", { name: /Add|Save|Create/ })
          .last(),
        { after: 600 },
      );
    };

    // Off camera: both sign up (Better Auth); each gets a personal label.
    await a.goto(origin);
    await a
      .getByRole("button", { name: "Create an account instead" })
      .waitFor({ timeout: 180_000 });
    await b.goto(origin);
    await signUp(b, "Bo", boEmail);
    await signUp(a, "Ada", adaEmail);
    await sleep(1000);

    await stage.start();
    await stage.full("a");
    stage.roll();
    await stage.title(
      "BigLabel",
      "Multi-tenant operations for record labels: members, roles, artists, releases. Next.js + Better Auth + Jazz.",
      2600,
    );
    await stage.caption(
      "Ada signed in with Better Auth; the server bootstrapped her personal label, with her as admin",
      3000,
    );
    await stage.caption("");

    await click(a, nav(a, "Settings"), { after: 800 });
    await click(a, a.getByText("Small", { exact: true }).first(), { after: 300 });
    await click(a, a.getByRole("button", { name: "Load demo data" }), { after: 300 });
    await a
      .getByText(/Loaded \d+ demo labels|already loaded/i)
      .first()
      .waitFor({ timeout: 120_000 });
    await stage.caption("Demo data: three more labels, with Ada as admin of each", 2400);
    await stage.caption("");

    await switchLabel(a);
    await click(a, nav(a, "Overview"), { after: 1200 });
    await stage.caption(`${LABEL}: counts and latest releases, each a live Jazz query`, 2800);
    await stage.caption("");

    await click(a, nav(a, "Releases"), { after: 1000 });
    const search = a.getByPlaceholder(/Search/i).first();
    await type(a, search, "the", { delay: 120 });
    await sleep(1000);
    await stage.caption("Search, filter, sort and paging are bounded, ordered queries", 2000);
    await search.fill("");
    await sleep(600);
    await stage.caption("");
    await click(a, a.locator("table tbody a").first(), { after: 1600 });
    await a.mouse.wheel(0, 300);
    await sleep(1000);

    await click(a, nav(a, "People"), { after: 900 });
    await click(a, a.getByRole("button", { name: "Add member" }), { after: 500 });
    await type(a, a.getByRole("dialog").getByLabel("Email"), boEmail, { delay: 25 });
    await click(a, a.getByRole("dialog").getByRole("combobox", { name: "Role" }), { after: 300 });
    await click(a, a.getByRole("option", { name: "Viewer" }).first(), { after: 300 });
    await stage.caption(
      "The server looks up the email, then writes the membership as Ada: permissions.ts decides",
    );
    await click(a, a.getByRole("dialog").getByRole("button", { name: "Add member" }), {
      after: 1200,
    });
    await a.getByText("Bo").first().waitFor({ timeout: 30_000 });
    await stage.caption("");

    await stage.split("a", "b", { scale: 0.6 });
    await openLabelMenu(b);
    await b
      .locator("[popover]:popover-open")
      .getByText(LABEL, { exact: true })
      .first()
      .waitFor({ timeout: 30_000 });
    await stage.caption(`${LABEL} appeared in Bo's label menu, live`, 1800);
    await click(b, b.locator("[popover]:popover-open").getByText(LABEL, { exact: true }).first(), {
      after: 800,
    });
    if (await b.locator("[popover]:popover-open").count()) await b.keyboard.press("Escape");
    await click(b, nav(b, "Artists"), { after: 1000 });
    await stage.caption("As a viewer, Bo can read the label but not change it", 2200);

    // A live filtered query: Bo searches; a matching artist appears as Ada adds it.
    await type(b, b.getByPlaceholder(/Search/i).first(), "aard", { delay: 110 });
    await stage.caption("Bo searches the artists for “aard”: nothing yet", 1800);
    await click(a, nav(a, "Artists"), { after: 600 });
    await stage.caption("Ada adds a matching artist…");
    await addArtist("Aardvark Choir");
    await b.getByText("Aardvark Choir").first().waitFor({ timeout: 30_000 });
    stage.poster();
    await stage.caption("…and it appears in Bo's filtered list immediately", 2600);

    // BigLabel waits for the server to confirm each write, so the one who
    // goes offline here is Bo, the reader.
    await stage.caption("Bo's Wi-Fi drops…");
    await stage.wifi("b", false);
    await stage.caption("…while Ada renames the artist");
    await click(a, a.getByRole("button", { name: "Edit" }), { after: 500 });
    await type(a, a.getByRole("dialog").getByLabel("Name"), "Aardvark Choir and Strings", {
      clear: true,
    });
    await click(
      a,
      a
        .getByRole("dialog")
        .getByRole("button", { name: /Save|Update/ })
        .last(),
      { after: 600 },
    );
    await a.getByRole("heading", { name: "Aardvark Choir and Strings" }).waitFor();
    await sleep(1200);
    if (await b.getByText("Aardvark Choir and Strings").count())
      throw new Error("A write reached Bo while his Wi-Fi was off");
    await stage.caption("Bo's list still has the old name", 1800);
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("b", true);
    await b.getByText("Aardvark Choir and Strings").first().waitFor({ timeout: 30_000 });
    await stage.caption("…and Bo's filtered list catches up", 2400);
    await stage.caption("");

    await stage.full("a");
    await click(a, nav(a, "Teams"), { after: 1400 });
    await stage.caption("Teams: groups of members with their own roles", 2000);
    await stage.caption("");
  },
});
