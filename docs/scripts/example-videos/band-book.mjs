// Examples-page walkthrough for BandBook (examples/band-book, Next.js + Better Auth).
//   node scripts/example-videos/band-book.mjs   # writes public/examples/videos/band-book.*
import { nextServer, record } from "./walkthrough.mjs";
import { click, sleep } from "./stage.mjs";

const port = 3461;
const origin = `http://127.0.0.1:${port}`;
const run = Date.now().toString(36);

await record({
  id: "band-book",
  app: "examples/band-book/apps/nextjs-betterauth",
  server: ({ dir }) =>
    nextServer({
      dir,
      port,
      // The example's local development secrets, from .env.example.
      env: {
        BACKEND_SECRET: "band-book-development-backend-secret",
        BETTER_AUTH_SECRET: "band-book-local-development-better-auth-secret",
      },
    }),
  async run(stage) {
    const device = { address: "bandbook.example.com" };
    const a = await stage.device("a", { ...device, name: "Ada's laptop" });
    const b = await stage.device("b", { ...device, name: "Bo's laptop", color: "#ea580c" });
    for (const page of [a, b]) page.setDefaultTimeout(90_000);
    const side = (page, name) => page.getByRole("button", { name, exact: true }).first();
    const title = (page) => page.getByRole("textbox", { name: "Page title" });
    // The block editor's text areas, found by what they contain.
    const textareaWith = async (page, needle, timeout = 30_000) => {
      const end = Date.now() + timeout;
      for (;;) {
        const i = await page
          .locator("textarea")
          .evaluateAll((els, n) => els.findIndex((e) => e.value.includes(n)), needle);
        if (i >= 0) return page.locator("textarea").nth(i);
        if (Date.now() > end) throw new Error(`No text block with ${needle}`);
        await sleep(200);
      }
    };
    const appendTo = async (page, needle, text) => {
      await click(page, await textareaWith(page, needle), { after: 200 });
      await page.keyboard.press("End");
      await page.keyboard.type(text, { delay: 70 });
    };

    // Off camera: Bo and Ada sign up, each on their own laptop.
    const signUp = async (page, name) => {
      await page.goto(origin);
      await page.getByRole("button", { name: "Create an account" }).click({ timeout: 180_000 });
      await page.getByLabel("Name").fill(name);
      await page.getByLabel("Email").fill(`${name.toLowerCase()}-${run}@band-book.test`);
      await page.getByLabel("Password").fill("correct horse battery");
      await page.getByRole("button", { name: "Create account" }).click();
      await page.waitForURL(/\/workspace/);
      await side(page, "Setlist: spring tour").waitFor();
    };
    await signUp(b, "Bo");
    await signUp(a, "Ada");

    await stage.start();
    await stage.full("a");
    stage.roll();
    await stage.title(
      "BandBook",
      "A Notion-style notebook for running a band, with an issue tracker inside. Next.js + Better Auth + Jazz.",
      2600,
    );
    await stage.caption(
      "On sign-up the server bootstrapped a demo band in one transaction: pages, songs, tour notes, issues",
      2600,
    );
    await stage.caption("");

    await click(a, side(a, "Setlist: spring tour"), { after: 1200 });
    await click(a, a.getByRole("checkbox", { name: "Night bus home" }).first(), { after: 900 });

    await click(a, side(a, "Songs"), { after: 800 });
    await click(a, side(a, "Harbour lights"), { after: 800 });
    await title(a).waitFor();
    await click(a, a.getByRole("button", { name: "Add block" }), { after: 400 });
    await click(a, a.getByRole("menuitem", { name: "Text" }), { after: 300 });
    await a.keyboard.type("Bridge: stay on the IV chord", { delay: 55 });
    await sleep(800);
    await stage.caption("Pages nest, and blocks nest inside blocks", 1800);
    await stage.caption("");

    await click(a, side(a, "Issues"), { after: 800 });
    await a.getByRole("table").waitFor();
    await stage.caption("The band's issue tracker is a database of pages: a table…", 2000);
    await click(a, a.getByText("Board", { exact: true }).first(), { after: 600 });
    await stage.caption("…and a board", 1800);
    await stage.caption("");

    await click(a, side(a, "Harbour lights"), { after: 800 });
    await click(a, a.getByRole("button", { name: "Share" }), { after: 800 });
    await click(a, a.getByRole("button", { name: "Create link" }), { after: 800 });
    const linkInput = a.getByRole("dialog").getByRole("textbox").last();
    await linkInput.waitFor();
    const inviteUrl = await linkInput.inputValue();
    await stage.caption(
      "A “Can edit” link for this song only. Row policies in permissions.ts enforce it.",
      2800,
    );
    await stage.caption("");
    await click(a, a.getByRole("button", { name: "Done" }), { after: 500 });

    await stage.split("a", "b", { scale: 0.62 });
    await stage.caption("Bo, signed in on his own laptop, opens the link");
    await b.goto(inviteUrl);
    await stage.recast("b");
    await b.waitForURL(/p=/);
    await title(b).waitFor();
    await sleep(600);
    await stage.caption(
      "Bo sees the shared song and the pages inside it, and nothing else of the band",
      2800,
    );

    await appendTo(a, "Key of D", " Count in on four.");
    await textareaWith(b, "Count in on four.");
    stage.poster();
    await stage.caption("Ada types, and Bo's copy updates as she types", 2000);

    await stage.caption("Bo's Wi-Fi drops…");
    await stage.wifi("b", false);
    await stage.caption("…and both keep editing the same song");
    await appendTo(b, "Bridge: stay on the IV chord", ", then back to D");
    await appendTo(a, "Count in on four.", " Tuning: drop D.");
    await sleep(1200);
    if (
      await b.locator("textarea").evaluateAll((els) => els.some((e) => e.value.includes("drop D")))
    )
      throw new Error("Ada's edit reached Bo while his Wi-Fi was off");
    await stage.caption("Each side has only its own edit for now", 1800);
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("b", true);
    await textareaWith(b, "drop D.");
    await textareaWith(a, "then back to D");
    await stage.caption(
      "…and both edits arrive. Each keystroke is a small text splice, so they merge.",
      3000,
    );
    await stage.caption("");
  },
});
