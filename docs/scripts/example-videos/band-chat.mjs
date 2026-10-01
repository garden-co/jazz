// Examples-page walkthrough for BandChat (examples/band-chat, Next.js + Better Auth).
//   node scripts/example-videos/band-chat.mjs   # writes public/examples/videos/band-chat.*
import { randomBytes } from "node:crypto";
import { nextServer, record } from "./walkthrough.mjs";
import { click, pointAt, sleep, type } from "./stage.mjs";

const port = 3465;
const origin = `http://127.0.0.1:${port}`;
const run = Date.now().toString(36);
const secret = () => randomBytes(32).toString("base64url");

// A small stage plot, drawn in the page, to attach to a message.
async function stagePlot(page) {
  const base64 = await page.evaluate(async () => {
    const canvas = new OffscreenCanvas(640, 400);
    const g = canvas.getContext("2d");
    g.fillStyle = "#23262d";
    g.fillRect(0, 0, 640, 400);
    g.fillStyle = "#e8e2d4";
    g.fillRect(40, 270, 560, 90);
    g.fillStyle = "#3b82f6";
    g.beginPath();
    g.arc(170, 190, 55, 0, 7);
    g.fill();
    g.fillStyle = "#f0a830";
    g.fillRect(290, 140, 100, 110);
    g.fillStyle = "#d94f4f";
    g.beginPath();
    g.arc(500, 200, 45, 0, 7);
    g.fill();
    g.fillStyle = "#ffffff";
    g.font = "600 30px sans-serif";
    g.fillText("Stage plot, Friday", 40, 64);
    const bytes = new Uint8Array(await (await canvas.convertToBlob()).arrayBuffer());
    let text = "";
    for (const b of bytes) text += String.fromCharCode(b);
    return btoa(text);
  });
  return Buffer.from(base64, "base64");
}

await record({
  id: "band-chat",
  app: "examples/band-chat/apps/nextjs-betterauth",
  server: ({ dir }) =>
    nextServer({
      dir,
      port,
      // Throwaway local secrets, as `pnpm dev` would generate.
      env: { BACKEND_SECRET: secret(), BETTER_AUTH_SECRET: secret() },
    }),
  async run(stage) {
    const device = { address: "bandchat.example.com" };
    const a = await stage.device("a", { ...device, name: "Olive's laptop" });
    const b = await stage.device("b", { ...device, name: "Gus's laptop", color: "#ea580c" });
    for (const page of [a, b]) page.setDefaultTimeout(90_000);
    const composer = (page) => page.getByRole("textbox", { name: "Message input" });
    const send = async (page, text) => {
      await type(page, composer(page), text, { delay: 35 });
      await page.keyboard.press("Enter");
      await page.getByText(text).first().waitFor();
    };
    const signUp = async (page, name, slow) => {
      const press = slow ? click : (p, l) => l.click();
      const fill = (label, text) =>
        slow
          ? type(page, page.getByLabel(label).first(), text, { delay: 35 })
          : page.getByLabel(label).first().fill(text);
      await press(page, page.getByRole("button", { name: "Create an account" }));
      await fill("Name", name);
      await fill("Email", `${name.split(" ")[0].toLowerCase()}-${run}@example.com`);
      await page.getByLabel("Password").first().fill("correct horse battery");
      await press(page, page.getByRole("button", { name: "Create account", exact: true }));
      await page.getByText("Set up your profile").waitFor({ timeout: 120_000 });
      await press(page, page.getByRole("button", { name: "Continue" }));
      await page.getByText("No rooms yet").waitFor({ timeout: 120_000 });
    };
    const invite = (page) => page.getByRole("button", { name: /^Invite/ }).first();

    // Off camera: Gus signs up on his own laptop.
    await b.goto(origin);
    await b.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 240_000 });
    await signUp(b, "Gus Moreno", false);
    await a.goto(origin);
    await a.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 120_000 });

    await stage.start();
    await stage.full("a");
    stage.roll();
    await stage.title(
      "BandChat",
      "A band's group chat: rooms, join requests, attachments. Next.js + Better Auth + Jazz.",
      2600,
    );
    await stage.caption("Olive signs up. Better Auth signs her in; Jazz enrols her from its JWT.");
    await signUp(a, "Olive Park", true);
    await stage.caption("");

    await click(a, a.getByRole("button", { name: "Create a room" }));
    await type(a, a.getByLabel("Room name"), "Rehearsal", { delay: 45 });
    await click(a, a.getByRole("button", { name: "Create room" }));
    await a.getByRole("heading", { name: "Rehearsal" }).waitFor();
    await stage.caption("Every message is a local write first, so it shows at once");
    await send(a, "Soundcheck moved to 7. Bring the new in-ear packs.");
    await pointAt(a, a.getByRole("button", { name: "Attach" }));
    await a
      .locator("input[aria-label='Attachment']")
      .setInputFiles([
        { name: "stage-plot.png", mimeType: "image/png", buffer: await stagePlot(a) },
      ]);
    await stage.caption("Attachments stream into the message row");
    await sleep(900);
    await a.keyboard.press("Enter");
    await a.getByText("stage-plot.png").first().waitFor({ state: "attached" });
    await sleep(1500);
    await stage.caption("");
    const roomId = new URL(a.url()).searchParams.get("room");

    await stage.split("a", "b", { scale: 0.62 });
    await stage.caption("Gus opens the room link. A link only lets him ask to join.");
    await b.goto(`${origin}/dashboard?join=${roomId}`);
    await stage.recast("b");
    await click(b, b.getByRole("button", { name: "Ask to join" }), { after: 300 });
    await b.getByText("Waiting for the room creator").waitFor();
    await invite(a).filter({ hasText: "1" }).waitFor({ timeout: 60_000 });
    await stage.caption("The request shows up for Olive, live", 1800);
    await click(a, invite(a), { after: 600 });
    await stage.caption("Only the room creator can admit: Jazz permissions enforce it, not the UI");
    await click(a, a.getByRole("button", { name: "Admit" }), { after: 1000 });
    await a.keyboard.press("Escape");

    // Gus's room list is a live query of the rooms he belongs to.
    await b.getByRole("heading", { name: "Rehearsal" }).waitFor({ timeout: 60_000 });
    await b.getByText("Soundcheck moved to 7").first().waitFor({ timeout: 60_000 });
    await stage.caption(
      "Admitted: the room appears in Gus's live room list, history and attachment included",
      2600,
    );
    await stage.caption("Gus replies, and it lands in Olive's window live");
    await send(b, "In. I'll bring the spare snare too.");
    await a.getByText("I'll bring the spare snare").first().waitFor({ timeout: 30_000 });
    stage.poster();
    await sleep(1600);

    await stage.caption("Gus's Wi-Fi drops…");
    await stage.wifi("b", false);
    await stage.caption("…he keeps chatting: the message commits on his laptop");
    await send(b, "Running 10 min late, start without me");
    await sleep(1200);
    if (await a.getByText("Running 10 min late").count())
      throw new Error("A message reached Olive while Gus's Wi-Fi was off");
    await stage.caption("Olive doesn't have it yet", 1800);
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("b", true);
    await a.getByText("Running 10 min late").first().waitFor({ timeout: 30_000 });
    await stage.caption("…and it arrives in Olive's room", 2400);
    await stage.caption("");

    await a.emulateMedia({ colorScheme: "dark" });
    await stage.full("a");
    await stage.caption("Dark mode", 2000);
    await stage.caption("");
  },
});
