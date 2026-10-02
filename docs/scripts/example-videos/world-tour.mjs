// Examples-page walkthrough for World Tour (examples/world-tour, Vue + Vite).
//   node scripts/example-videos/world-tour.mjs   # writes public/examples/videos/world-tour.*
import { record } from "./walkthrough.mjs";
import { click as stageClick, sleep, type as stageType } from "./stage.mjs";

// The globe animates every frame on a software renderer, so point quickly and
// click directly.
const fast = { steps: 2, hover: 150, direct: true };
const click = (page, locator, options) => stageClick(page, locator, { ...fast, ...options });
const type = (page, locator, text, options) =>
  stageType(page, locator, text, { ...fast, ...options });

const port = 5391;
const url = `http://127.0.0.1:${port}/`;

await record({
  id: "world-tour",
  // The globe is WebGL.
  webgl: true,
  app: "examples/world-tour",
  server: () => ({
    command: "pnpm",
    args: ["exec", "vite", "dev", "--port", String(port), "--strictPort", "--host", "127.0.0.1"],
    // A fresh in-memory server (so the demo tour is new) and no dev inspector toggle.
    env: { VITE_E2E: "true" },
    ready: /Local:\s+http/,
  }),
  async run(stage) {
    const device = { address: "worldtour.example.com", keepPorts: [port] };
    const a = await stage.device("a", { ...device, name: "Tour manager's laptop" });
    const b = await stage.device("b", { ...device, name: "Fan's laptop", color: "#ea580c" });
    const p = await stage.device("p", {
      ...device,
      kind: "phone",
      color: "#ea580c",
      viewport: { width: 375, height: 732 },
      colorScheme: "dark",
      contextOptions: { isMobile: true, hasTouch: true, deviceScaleFactor: 1 },
    });
    const pin = (page, name) => page.locator(`.stop-pin[aria-label="${name}"]`);
    const confirm = async (name) => {
      await pin(a, name).dispatchEvent("click");
      await a.locator(".stop-detail").waitFor();
      await sleep(600);
      await click(a, a.getByRole("button", { name: "Edit stop" }));
      await a.locator(".stop-detail select").selectOption("confirmed");
      await sleep(500);
      await click(a, a.getByRole("button", { name: "Save" }));
    };

    // The first visitor to an empty server gets the seeded demo tour.
    await a.goto(url);
    await a.getByText("Your band.").waitFor({ timeout: 90_000 });
    await a.locator(".stop-pin").nth(11).waitFor({ state: "attached", timeout: 60_000 });
    await sleep(2000);

    await stage.start();
    await stage.full("a");
    stage.roll();
    await stage.title(
      "World Tour",
      "A band plans its tour on a globe; fans follow the confirmed dates. Vue + Jazz.",
      2600,
    );
    await stage.caption("The tour manager's view: all 12 stops, tentative ones included", 1800);
    await click(a, a.getByRole("button", { name: "Play tour" }), { after: 3000 });
    await click(a, a.getByRole("button", { name: "Stop tour" }), { after: 600 });
    await stage.caption("");

    const ownerPins = await a
      .locator(".stop-pin")
      .evaluateAll((els) => els.map((e) => e.getAttribute("aria-label")));
    await a.locator(".stop-pin").first().dispatchEvent("click");
    await a.locator(".stop-detail").waitFor();
    await stage.caption(
      "A stop: calendar, venue, and private notes only band members can read",
      2600,
    );
    await stage.caption("");
    await click(a, a.getByRole("button", { name: "Close" }), { after: 900 });

    await click(a, a.getByRole("button", { name: "Band", exact: true }), { after: 900 });
    const inviteLink = await a.getByLabel("Invite link").inputValue();
    const publicLink = inviteLink.replace(/\/join\/.*$/, "");
    await stage.caption("Only the owner can read the invite code", 1800);
    await stage.caption("");
    await click(a, a.getByRole("button", { name: "Close" }), { after: 600 });

    // A fan with the public link, on another laptop with its own account.
    await b.goto(publicLink);
    await stage.recast("b");
    await b.getByRole("dialog").waitFor({ timeout: 60_000 });
    await sleep(1500);
    await stage.split("a", "b", { scale: 0.62 });
    await stage.caption(
      "A fan with the public link gets only confirmed dates: the server filters the rows",
      3000,
    );
    await click(b, b.getByRole("button", { name: "Explore the globe" }), { after: 1000 });
    await stage.caption("");
    const publicPins = await b
      .locator(".stop-pin")
      .evaluateAll((els) => els.map((e) => e.getAttribute("aria-label")));
    const hidden = ownerPins.filter((n) => !publicPins.includes(n));
    if (hidden.length < 2) throw new Error(`Expected two tentative stops, got ${hidden.length}`);

    // A row starts matching the fan's filtered query: it appears live.
    await stage.caption(`Confirm “${hidden[0]}”…`);
    await confirm(hidden[0]);
    await pin(b, hidden[0]).waitFor({ state: "attached", timeout: 30_000 });
    await stage.caption("…and it shows up on the fan's globe, live", 1200);
    await pin(b, hidden[0]).dispatchEvent("click");
    stage.poster();
    await sleep(2200);
    await stage.caption("");
    await click(a, a.getByRole("button", { name: "Close" }), { after: 300 });
    await b
      .getByRole("button", { name: "Close" })
      .first()
      .click()
      .catch(() => {});

    // Wi-Fi off: the change waits on the laptop until it's back online.
    await stage.caption("On the road, the tour manager's Wi-Fi drops…");
    await stage.wifi("a", false);
    await stage.caption(`…confirming “${hidden[1]}” still works, locally`);
    await confirm(hidden[1]);
    await sleep(1500);
    if (await pin(b, hidden[1]).count())
      throw new Error("An offline confirmation reached the fan while Wi-Fi was off");
    await stage.caption("The fan doesn't see it yet", 1800);
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("a", true);
    await pin(b, hidden[1]).waitFor({ state: "attached", timeout: 30_000 });
    await stage.caption("…and the confirmed date reaches the fan", 2400);
    await stage.caption("");
    await click(a, a.getByRole("button", { name: "Close" }), { after: 300 });

    // The fan joins with the invite link and becomes a member.
    await stage.full("a");
    await click(a, a.getByRole("button", { name: "Band", exact: true }), { after: 300 });
    await stage.caption("The tour manager sends the fan the invite link");
    await b.goto(inviteLink);
    await stage.recast("b");
    await b.getByRole("dialog", { name: /^Join / }).waitFor({ timeout: 60_000 });
    await stage.split("a", "b", { scale: 0.62 });
    await stage.caption("The fan opens it", 800);
    await type(b, b.getByLabel("Your name"), "Robin");
    await click(b, b.getByRole("button", { name: "Join band" }));
    await b.getByText("You're in this band.").waitFor({ timeout: 30_000 });
    await a.locator(".member-list li", { hasText: "Robin" }).waitFor({ timeout: 30_000 });
    await stage.caption(
      "The server accepts the membership; the owner's member list updates live",
      2200,
    );
    await b
      .locator(".stop-pin")
      .nth(ownerPins.length - 1)
      .waitFor({ state: "attached", timeout: 30_000 });
    await stage.caption("As a member, Robin now sees every stop and the private notes", 2500);
    await stage.caption("");

    // Phone. Stop the other two globes first so the phone renders smoothly.
    await a.goto("about:blank");
    await b.goto("about:blank");
    await p.goto(publicLink);
    await stage.recast("p");
    await p.getByRole("dialog").waitFor({ timeout: 60_000 });
    await sleep(1200);
    await stage.show([{ id: "p", x: 452, y: 20, w: 375, h: 760 }]);
    await stage.caption("The public tour page on a phone", 1800);
    await click(p, p.getByRole("button", { name: "Explore the globe" }), { after: 4500 });
    await stage.caption("");
  },
});
