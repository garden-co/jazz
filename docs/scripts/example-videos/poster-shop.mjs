// Examples-page walkthrough for PosterShop (examples/poster-shop, Next.js + Better Auth).
//   node scripts/example-videos/poster-shop.mjs   # writes public/examples/videos/poster-shop.*
import { nextServer, record } from "./walkthrough.mjs";
import { click, pointAt, sleep, type } from "./stage.mjs";

const port = 3464;
const origin = `http://127.0.0.1:${port}`;
const uniq = Date.now().toString(36);

await record({
  id: "poster-shop",
  app: "examples/poster-shop/apps/nextjs-betterauth",
  server: ({ dir }) =>
    nextServer({
      dir,
      port,
      // The example's local development secrets, from .env.example.
      env: {
        BACKEND_SECRET: "poster-shop-development-backend-secret",
        BETTER_AUTH_SECRET: "2SNhYRceYvKf1HnJ7mQxB3aWd6LeP9tR4uCg8Vz0Ds5FiOoAbXkMwZq",
      },
    }),
  async run(stage) {
    const device = { address: "postershop.example.com" };
    const a = await stage.device("a", { ...device, name: "Ada's laptop" });
    const b = await stage.device("b", { ...device, name: "Grace's laptop", color: "#ea580c" });
    for (const page of [a, b]) page.setDefaultTimeout(90_000);
    const canvas = (page) => page.getByRole("application", { name: "Poster canvas" });
    const shape = (page, id) => canvas(page).locator(`[data-shape-id="${id}"]`);
    const swatch = (page, name) =>
      page
        .getByRole("radio", { name, exact: true })
        .or(page.getByRole("button", { name, exact: true }))
        .first();
    const signUp = async (page, name, slow) => {
      const press = slow ? click : (p, l) => l.click();
      const fill = (label, text) =>
        slow
          ? type(page, page.getByLabel(label), text, { delay: 35 })
          : page.getByLabel(label).fill(text);
      await press(page, page.getByRole("button", { name: "Create an account" }));
      await fill("Name", name);
      await fill("Email", `${name.toLowerCase()}-${uniq}@poster.test`);
      await page.getByLabel("Password").fill("poster-shop-demo");
      await press(page, page.getByRole("button", { name: "Create account" }));
      await canvas(page).waitFor({ timeout: 120_000 });
    };
    // Drags a shape by its centre, the way a person would.
    const drag = async (page, locator, dx, dy) => {
      const box = await pointAt(page, locator);
      await page.mouse.down();
      await page.mouse.move(box.x + box.width / 2 + dx, box.y + box.height / 2 + dy, { steps: 18 });
      await page.mouse.up();
      await sleep(400);
    };

    // Off camera: Grace signs up on her own laptop.
    await b.goto(origin);
    await b.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 240_000 });
    await signUp(b, "Grace", false);
    await a.goto(origin);
    await a.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 120_000 });
    await signUp(a, "Ada", false);

    await stage.start();
    await stage.full("a");
    stage.roll();
    await stage.title(
      "PosterShop",
      "Design gig posters together: layers, shapes, images and checkpoints. Next.js + Better Auth + Jazz.",
      2600,
    );
    await stage.caption(
      "Ada's first sign-in seeded a demo poster: layers, an SVG artboard and an inspector",
      2600,
    );

    // The sun: the inner ellipse of the artwork layer.
    const sun = await canvas(a)
      .locator('[data-kind="ellipse"]')
      .nth(1)
      .getAttribute("data-shape-id");
    await click(a, shape(a, sun), { after: 500 });
    await stage.caption(
      "Select, recolour, add and move shapes. Every change is a local write that syncs.",
    );
    await click(a, swatch(a, "Sun"), { after: 500 });
    await click(a, a.getByRole("button", { name: "Add rectangle" }), { after: 500 });
    const added = await canvas(a)
      .locator('[data-kind="rect"]')
      .last()
      .getAttribute("data-shape-id");
    await drag(a, shape(a, added), 140, -260);
    await click(a, swatch(a, "Pink"), { after: 800 });
    await stage.caption("");

    await click(a, a.getByRole("button", { name: "Invite" }), { after: 600 });
    await click(a, a.getByRole("button", { name: "Create invite link" }), { after: 600 });
    const link = await a.getByLabel("Invite link").inputValue();
    await stage.caption("An invite link: can edit or can view, single use by default", 2400);
    await a.keyboard.press("Escape");
    await sleep(400);
    await stage.caption("");

    await stage.split("a", "b", { scale: 0.62 });
    await stage.caption("Grace opens the link on her laptop and joins as an editor");
    // The link differs from Grace's page only by its #fragment; load it fresh.
    await b.goto("about:blank");
    await b.goto(link);
    await stage.recast("b");
    await shape(b, sun).waitFor({ timeout: 90_000 });
    await sleep(1000);

    await stage.caption("Her cursor shows up on Ada's poster, live");
    const sunBox = await shape(b, sun).boundingBox();
    for (const [dx, dy] of [
      [-60, -40],
      [40, -10],
      [10, 40],
    ])
      await b.mouse.move(sunBox.x + sunBox.width / 2 + dx, sunBox.y + sunBox.height / 2 + dy, {
        steps: 10,
      });
    await sleep(1500);
    await stage.caption("Grace recolours the sun, and the change arrives in Ada's window");
    const sunBefore = await shape(a, sun).getAttribute("fill");
    await click(b, shape(b, sun), { after: 400 });
    await click(b, swatch(b, "Violet"), { after: 300 });
    for (let i = 0; i < 60 && (await shape(a, sun).getAttribute("fill")) === sunBefore; i++)
      await sleep(250);
    stage.poster();
    await sleep(1800);

    await stage.caption("Grace's Wi-Fi drops…");
    await stage.wifi("b", false);
    await stage.caption("…she keeps designing: moves and recolours Ada's new shape");
    const before = await shape(a, added).evaluate((el) => el.outerHTML);
    await click(b, shape(b, added), { after: 300 });
    await click(b, swatch(b, "Teal"), { after: 300 });
    await drag(b, shape(b, added), -120, 60);
    await sleep(1200);
    if ((await shape(a, added).evaluate((el) => el.outerHTML)) !== before)
      throw new Error("An offline edit reached Ada while Grace's Wi-Fi was off");
    await stage.caption("Ada doesn't have those edits yet", 1800);
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("b", true);
    for (
      let i = 0;
      i < 120 && (await shape(a, added).evaluate((el) => el.outerHTML)) === before;
      i++
    )
      await sleep(250);
    if ((await shape(a, added).evaluate((el) => el.outerHTML)) === before)
      throw new Error("Grace's offline edits never reached Ada");
    await stage.caption("…and they sync to Ada's poster", 2400);
    await stage.caption("");

    await stage.full("a");
    await click(
      a,
      a
        .getByRole("tab", { name: "History" })
        .or(a.getByText("History", { exact: true }))
        .first(),
      { after: 600 },
    );
    await type(a, a.getByLabel("Checkpoint name"), "First draft");
    await click(a, a.getByRole("button", { name: "Save" }), { after: 1000 });
    await stage.caption("Named checkpoints keep a copy of the poster to preview later", 2400);
    await stage.caption("");
  },
});
