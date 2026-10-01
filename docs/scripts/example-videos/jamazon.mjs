// Examples-page walkthrough for Jamazon (examples/jamazon, Next.js + Better Auth).
//   node scripts/example-videos/jamazon.mjs   # writes public/examples/videos/jamazon.*
import { randomBytes } from "node:crypto";
import { nextServer, record } from "./walkthrough.mjs";
import { click, sleep, type } from "./stage.mjs";

const port = 3466;
const origin = `http://127.0.0.1:${port}`;
const email = `ada-${Date.now().toString(36)}@jamazon.test`;
const password = "jamazon-demo-2026";
const PRODUCT = "Brass snare, 14 × 5.5 in";
const secret = () => randomBytes(32).toString("base64url");

await record({
  id: "jamazon",
  app: "examples/jamazon/apps/nextjs-betterauth",
  server: ({ dir }) =>
    nextServer({
      dir,
      port,
      // Throwaway local secrets, as `pnpm dev` would generate; sandbox payments.
      env: { BACKEND_SECRET: secret(), BETTER_AUTH_SECRET: secret() },
    }),
  async run(stage) {
    const a = await stage.device("a", { address: "jamazon.example.com", name: "Ada's laptop" });
    const p = await stage.device("p", {
      kind: "phone",
      color: "#ea580c",
      viewport: { width: 340, height: 652 },
      contextOptions: { isMobile: true, hasTouch: true, deviceScaleFactor: 1 },
    });
    for (const page of [a, p]) page.setDefaultTimeout(90_000);
    const quantity = (page) =>
      page.getByRole("spinbutton", { name: `Quantity of ${PRODUCT}`, exact: true });
    // Steps the quantity with the + and − buttons, as a person would.
    const setQuantity = async (page, n) => {
      for (let i = 0; i < 20; i++) {
        const now = Number(await quantity(page).inputValue());
        if (now === n) return;
        const step = now < n ? "Increment" : "Decrement";
        await click(page, page.getByRole("button", { name: `${step} Quantity of ${PRODUCT}` }), {
          after: 350,
          direct: true,
        });
      }
    };
    const hasQuantity = async (page, n) => (await quantity(page).inputValue()) === String(n);
    const until = async (check, ms = 30_000) => {
      const end = Date.now() + ms;
      while (Date.now() < end) {
        if (await check().catch(() => false)) return true;
        await sleep(250);
      }
      return false;
    };
    // Laptop and phone together; the phone keeps its own size.
    const together = () =>
      stage.show([
        { id: "a", x: 20, y: 20, w: 860, h: 690, scale: 0.72 },
        { id: "p", x: 912, y: 20, w: 348, h: 690 },
      ]);

    // Off camera: compile the store, the cart, sign-in, checkout and orders.
    for (const path of ["/", "/cart", "/sign-in", "/checkout", "/orders"]) {
      await a.goto(origin + path);
      await a.getByRole("heading").first().waitFor({ timeout: 240_000 });
    }
    await a.goto(origin);
    await a.getByPlaceholder("Search the store").waitFor({ timeout: 120_000 });

    await stage.start();
    await stage.full("a");
    stage.roll();
    await stage.title(
      "Jamazon",
      "A music-gear store: catalogue, cart, checkout and orders. Next.js + Better Auth + Jazz.",
      2600,
    );
    await stage.caption("Search is a live query over the local catalogue: each key narrows it");
    await type(a, a.getByPlaceholder("Search the store"), "snare", { delay: 160 });
    await a.getByRole("link", { name: PRODUCT }).waitFor();
    await sleep(1200);
    await stage.caption("");
    await click(a, a.getByRole("link", { name: PRODUCT }), { after: 800, direct: true });
    await a.getByRole("button", { name: "Add to cart" }).waitFor();
    await click(a, a.getByRole("button", { name: "Add to cart" }), { after: 600 });
    await stage.caption("Ada hasn't signed up: this is a guest cart, kept on her device", 2200);
    await click(a, a.getByRole("link", { name: /^Cart/ }).first(), { after: 800 });
    await quantity(a).waitFor();
    await stage.caption("");

    await click(a, a.getByRole("link", { name: "Sign in" }).first(), { after: 600 });
    await click(a, a.getByText("Create account", { exact: true }).first(), { after: 300 });
    await type(a, a.getByLabel("Name"), "Ada", { delay: 60 });
    await type(a, a.getByLabel("Email"), email, { delay: 25 });
    await a.getByLabel("Password").fill(password);
    await stage.caption("She creates an account, and the guest cart becomes her account's cart");
    await click(a, a.getByRole("button", { name: "Create account" }).last(), { after: 600 });
    await a.waitForURL((url) => !url.pathname.startsWith("/sign-in"), { timeout: 120_000 });
    // Off camera: Ada signs in on her phone, meanwhile.
    const phone = (async () => {
      await p.goto(`${origin}/sign-in?next=/cart`);
      await p.getByLabel("Email").fill(email);
      await p.getByLabel("Password").fill(password);
      await p.getByRole("button", { name: "Sign in" }).last().click();
      await p.waitForURL((url) => !url.pathname.startsWith("/sign-in"), { timeout: 120_000 });
      await p.goto(`${origin}/cart`);
      await quantity(p).waitFor();
    })();
    await a.goto(`${origin}/cart`);
    await stage.recast("a");
    await quantity(a).waitFor();
    await stage.caption("Signed in, her cart is part of her account, on every device", 2400);
    await phone;
    await stage.recast("p");
    await stage.caption("");

    await together();
    await stage.caption("On her phone, signed in: the same cart", 2000);
    await stage.caption("She bumps the quantity on the laptop, and the phone follows");
    await setQuantity(a, 2);
    if (!(await until(() => hasQuantity(p, 2))))
      throw new Error("Quantity 2 never reached the phone");
    stage.poster();
    await sleep(1600);

    await stage.caption("The phone's Wi-Fi drops…");
    await stage.wifi("p", false);
    await stage.caption("…and she changes the quantity there, offline");
    await setQuantity(p, 3);
    await sleep(1500);
    if (!(await hasQuantity(a, 2))) throw new Error("An offline edit reached the laptop");
    await stage.caption("The laptop still says 2", 1800);
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("p", true);
    if (!(await until(() => hasQuantity(a, 3), 45_000)))
      throw new Error("The phone's offline edit never reached the laptop");
    await stage.caption("…and the laptop catches up", 2200);

    await p.goto(`${origin}/orders`);
    await stage.recast("p");
    await p.getByRole("heading", { name: "Orders", exact: true }).first().waitFor();
    await stage.caption("Checkout on the laptop. Her phone has her order list open.");
    await click(
      a,
      a
        .getByRole("link", { name: "Check out" })
        .or(a.getByRole("button", { name: "Check out" }))
        .first(),
      {
        after: 800,
      },
    );
    await type(a, a.getByLabel("Full name"), "Ada Lovelace", { delay: 30 });
    await type(a, a.getByRole("textbox", { name: /^Address/ }).first(), "12 Harbour Street", {
      delay: 30,
    });
    await type(a, a.getByLabel("City"), "Bristol", { delay: 30 });
    await type(a, a.getByLabel("Postcode"), "BS1 4QA", { delay: 30 });
    await type(a, a.getByLabel("Country"), "United Kingdom", { delay: 30 });
    await click(a, a.getByRole("button", { name: "Continue to review" }), { after: 900 });
    await click(a, a.getByRole("button", { name: "Place order" }), { after: 300 });
    await a.waitForURL(/\/orders\//, { timeout: 60_000 });
    await p.getByText("Awaiting payment").first().waitFor({ timeout: 30_000 });
    await stage.caption("The order shows up on her phone, live: a query of her own orders", 2400);

    await stage.caption("Sandbox payments: first the card is declined…");
    await click(a, a.getByText("Decline", { exact: true }), { after: 300 });
    await click(a, a.getByRole("button", { name: /^Pay / }), { after: 300 });
    await a.getByText("Payment failed").first().waitFor({ timeout: 30_000 });
    await sleep(1600);
    await stage.caption("…then approved");
    await click(a, a.getByText("Approve", { exact: true }), { after: 300 });
    await click(a, a.getByRole("button", { name: /^Pay / }), { after: 300 });
    await p.getByText("Paid", { exact: true }).first().waitFor({ timeout: 30_000 });
    await stage.caption(
      "A backend worker subscribes to paid orders and ships them. Both screens follow.",
    );
    await p.getByText("Shipped", { exact: true }).first().waitFor({ timeout: 60_000 });
    await sleep(2400);
    await stage.caption("");

    await a.emulateMedia({ colorScheme: "dark" });
    await stage.full("a");
    await click(a, a.getByRole("link", { name: "Drums" }).first(), { after: 1200 });
    await stage.caption("Dark mode", 2000);
    await stage.caption("");
  },
});
