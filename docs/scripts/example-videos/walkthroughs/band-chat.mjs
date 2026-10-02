// BandChat (examples/band-chat, Next.js + Better Auth): server and actions for
// band-chat.storyboard.ts.
import { randomBytes } from "node:crypto";
import { click, pointAt, type } from "../stage.mjs";
import { nextServer } from "../walkthrough.mjs";

const port = 3465;
const origin = `http://127.0.0.1:${port}`;
const run = Date.now().toString(36);
const secret = () => randomBytes(32).toString("base64url");

export const app = "examples/band-chat/apps/nextjs-betterauth";
export const server = ({ dir }) =>
  nextServer({
    dir,
    port,
    // Throwaway local secrets, as `pnpm dev` would generate.
    env: { BACKEND_SECRET: secret(), BETTER_AUTH_SECRET: secret() },
  });

const composer = (page) => page.getByRole("textbox", { name: "Message input" });
const invite = (page) => page.getByRole("button", { name: /^Invite/ }).first();

export const actions = {
  /** Off camera: a new account, its profile, and an empty room list. */
  async signUp({ page }, name) {
    await page.goto(origin);
    await page.getByRole("button", { name: "Create an account" }).waitFor({ timeout: 240_000 });
    await page.getByRole("button", { name: "Create an account" }).click();
    await page.getByLabel("Name").first().fill(name);
    await page
      .getByLabel("Email")
      .first()
      .fill(`${name.split(" ")[0].toLowerCase()}-${run}@example.com`);
    await page.getByLabel("Password").first().fill("correct horse battery");
    await page.getByRole("button", { name: "Create account", exact: true }).click();
    await page.getByText("Set up your profile").waitFor({ timeout: 120_000 });
    await page.getByRole("button", { name: "Continue" }).click();
    await page.getByText("No rooms yet").waitFor({ timeout: 120_000 });
  },

  async createRoom({ page, state }, name) {
    await click(page, page.getByRole("button", { name: "Create a room" }));
    await type(page, page.getByLabel("Room name"), name, { delay: 45 });
    await click(page, page.getByRole("button", { name: "Create room" }));
    await page.getByRole("heading", { name }).waitFor();
    state.roomId = new URL(page.url()).searchParams.get("room");
  },

  async send({ page }, text) {
    await type(page, composer(page), text, { delay: 35 });
    await page.keyboard.press("Enter");
    await page.getByText(text).first().waitFor();
  },

  /** Points at Attach and picks a stage plot drawn in the page. */
  async pickStagePlot({ page }) {
    await pointAt(page, page.getByRole("button", { name: "Attach" }));
    await page
      .locator("input[aria-label='Attachment']")
      .setInputFiles([
        { name: "stage-plot.png", mimeType: "image/png", buffer: await stagePlot(page) },
      ]);
  },

  async sendAttachment({ page }, name) {
    await page.keyboard.press("Enter");
    await page.getByText(name).first().waitFor({ state: "attached" });
  },

  async askToJoin({ stage, on, page, state }) {
    await page.goto(`${origin}/dashboard?join=${state.roomId}`);
    await stage.recast(on);
    await click(page, page.getByRole("button", { name: "Ask to join" }), { after: 300 });
    await page.getByText("Waiting for the room creator").waitFor();
  },

  async waitForJoinRequest({ page }) {
    await invite(page).filter({ hasText: "1" }).waitFor({ timeout: 60_000 });
  },

  async openInvites({ page }) {
    await click(page, invite(page), { after: 600 });
  },

  async admit({ page }) {
    await click(page, page.getByRole("button", { name: "Admit" }), { after: 1000 });
    await page.keyboard.press("Escape");
  },

  async waitForRoom({ page }, room, message) {
    await page.getByRole("heading", { name: room }).waitFor({ timeout: 60_000 });
    await page.getByText(message).first().waitFor({ timeout: 60_000 });
  },
};

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
