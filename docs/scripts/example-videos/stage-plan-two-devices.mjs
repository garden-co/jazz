// Records the homepage walkthrough: StagePlan on two independent "devices"
// side by side, a crew chief and a crew member on the same show board,
// syncing through the local Jazz server that the example's Vite plugin starts.
//
//   pnpm build:core                              # jazz-tools, WASM and NAPI
//   pnpm --filter docs capture:example-videos    # writes docs/public/examples/videos/
//
// Each device is its own browser context, so it has its own IndexedDB,
// local-first identity and Jazz client. Nothing is mocked: every change
// travels device → server → device. ffmpeg composes the two recordings.
// The output stays small (under 5 MB), so it is committed with the docs.

import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { composeWindows } from "./encode.mjs";

const exampleDir = fileURLToPath(
  new URL("../../../examples/stage-plan/apps/react-localfirst/", import.meta.url),
);
const outDir = fileURLToPath(new URL("../../public/examples/videos/", import.meta.url));
const id = "stage-plan-two-devices";
const port = Number(process.env.EXAMPLE_PORT ?? 5199);
const size = { width: 600, height: 700 };
const url = `http://127.0.0.1:${port}/`;
// Glide time for the drawn cursor between targets.
const glideMs = 275;

// Headless recordings have no visible pointer, so each device draws its own:
// an arrow that glides to wherever Playwright moves the mouse, with a ripple
// on every press. It ignores pointer events, so the app is driven as normal.
function installCursor({ color, glideMs }) {
  const mount = () => {
    const cursor = document.createElement("div");
    cursor.setAttribute("aria-hidden", "true");
    cursor.style.cssText = `position:fixed;left:0;top:0;z-index:2147483647;pointer-events:none;transform:translate(300px,470px);transition:transform ${glideMs}ms cubic-bezier(.3,.7,.3,1);`;
    cursor.innerHTML = `<svg width="22" height="28" viewBox="0 0 22 28" style="display:block;filter:drop-shadow(0 1px 2px rgba(0,0,0,.35))"><path d="M2 2 L2 23 L7.5 17.5 L11.5 26 L15 24.4 L11.2 16.2 L19 16.2 Z" fill="${color}" stroke="white" stroke-width="1.6" stroke-linejoin="round"/></svg>`;
    document.documentElement.append(cursor);
    const follow = (e) =>
      (cursor.style.transform = `translate(${e.clientX - 2}px,${e.clientY - 2}px)`);
    // A native drag sends dragover instead of mousemove.
    for (const type of ["mousemove", "dragover"]) addEventListener(type, follow, true);
    addEventListener(
      "mousedown",
      (e) => {
        const ripple = document.createElement("div");
        ripple.style.cssText = `position:fixed;left:${e.clientX - 16}px;top:${e.clientY - 16}px;width:32px;height:32px;border-radius:50%;border:3px solid ${color};z-index:2147483646;pointer-events:none;transition:transform 450ms ease-out,opacity 450ms ease-out;transform:scale(.3);opacity:.9;`;
        document.documentElement.append(ripple);
        requestAnimationFrame(() =>
          requestAnimationFrame(() => {
            ripple.style.transform = "scale(1.4)";
            ripple.style.opacity = "0";
          }),
        );
        setTimeout(() => ripple.remove(), 600);
      },
      true,
    );
  };
  if (document.readyState === "loading") addEventListener("DOMContentLoaded", mount);
  else mount();
}

// The dev-only inspector overlay (a floating toggle) isn't part of the app.
function hideDevOverlay() {
  const style = document.createElement("style");
  style.textContent = "jazz-inspector-overlay{display:none!important}";
  // Init scripts can run before the document has a root element.
  if (document.documentElement) document.documentElement.append(style);
  else addEventListener("DOMContentLoaded", () => document.head.append(style));
}

async function startExample() {
  // Start from an empty sync server so each recording starts from a fresh demo show.
  await rm(`${exampleDir}node_modules/.cache/jazz-dev-server`, { recursive: true, force: true });
  const child = spawn(
    "pnpm",
    ["exec", "vite", "--port", String(port), "--strictPort", "--host", "127.0.0.1"],
    { cwd: exampleDir, stdio: ["ignore", "pipe", "pipe"], detached: true },
  );
  let output = "";
  child.stdout.on("data", (chunk) => (output += chunk));
  child.stderr.on("data", (chunk) => (output += chunk));
  const ready = new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error(`Example did not start:\n${output}`)),
      120_000,
    );
    const poll = setInterval(() => {
      if (/Local:\s+http/.test(output)) {
        clearInterval(poll);
        clearTimeout(timeout);
        resolve();
      } else if (child.exitCode !== null) {
        clearInterval(poll);
        clearTimeout(timeout);
        reject(new Error(`Example exited:\n${output}`));
      }
    }, 250);
  });
  // Kill the whole group: Vite plus the Jazz server it manages.
  const stop = () => {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {}
  };
  return { ready, stop, log: () => output };
}

// The board is dense, so each device shows its pages at 80%. The zoom sits on
// <body>, which keeps the drawn cursor (on <html>) in viewport coordinates.
function zoomOut() {
  const style = document.createElement("style");
  style.textContent = "body{zoom:.8}";
  if (document.documentElement) document.documentElement.append(style);
  else addEventListener("DOMContentLoaded", () => document.head.append(style));
}

const example = await startExample();
const recordingDir = await mkdtemp(join(tmpdir(), "example-video-"));
let browser;
try {
  await example.ready;
  browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined });
  const devices = [];
  for (const [label, color] of [
    ["Mia, crew chief", "#2563eb"],
    ["Cole, crew", "#ea580c"],
  ]) {
    const context = await browser.newContext({
      viewport: size,
      colorScheme: "dark",
      recordVideo: { dir: join(recordingDir, String(devices.length)), size },
    });
    await context.addInitScript(installCursor, { color, glideMs });
    await context.addInitScript(hideDevOverlay);
    await context.addInitScript(zoomOut);
    const page = await context.newPage();
    devices.push({ label, context, page, startedAt: Date.now() });
  }
  const [a, b] = devices.map((d) => d.page);

  // Off camera (the recordings are trimmed to start at `origin`): Mia's first
  // run creates the demo show, and Cole joins it through Mia's invite link.
  const rename = async (page, name) => {
    await page.getByRole("button", { name: /^Stagehand / }).click();
    await page.getByRole("textbox", { name: "Name" }).fill(name);
    await page.getByRole("button", { name: "Save" }).click();
    await page.getByRole("button", { name }).waitFor();
  };
  await a.goto(url);
  const showLink = a.getByRole("link", { name: "The Late Lanterns: album launch" });
  await showLink.waitFor({ timeout: 60_000 });
  await rename(a, "Mia");
  const board = url + (await showLink.getAttribute("href"));
  await a.goto(`${board}/crew`);
  const inviteLink = a.getByRole("textbox", { name: "Invite link" });
  await inviteLink.and(a.locator(":not([value=''])")).waitFor();
  await b.goto(await inviteLink.inputValue());
  await b.getByRole("button", { name: "Add task" }).waitFor({ timeout: 60_000 });
  await rename(b, "Cole");
  await a.goto(board);
  await a.getByRole("link", { name: "Crew (2)" }).waitFor();
  for (const page of [a, b]) {
    await page.getByRole("link", { name: "Soundcheck with the band" }).waitFor();
    const hidden = await page.evaluate(() => {
      const overlay = document.querySelector("jazz-inspector-overlay");
      return !overlay || getComputedStyle(overlay).display === "none";
    });
    if (!hidden) throw new Error("The dev overlay is still visible");
  }
  await a.waitForTimeout(800);

  const origin = Date.now();
  const captions = [];
  const caption = async (text, pause = 900) => {
    const now = Date.now() - origin;
    if (captions.length) captions.at(-1).to = now;
    captions.push({ text, from: now, to: now });
    await a.waitForTimeout(pause);
  };
  // Glide the drawn cursor to the target's centre before acting on it.
  const pointAt = async (page, locator) => {
    const box = await locator.boundingBox();
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.waitForTimeout(glideMs + 100);
  };
  const click = async (page, locator) => {
    await pointAt(page, locator);
    await locator.click();
  };
  const add = async (page, title) => {
    const input = page.getByRole("textbox", { name: "New task" });
    await click(page, input);
    await input.pressSequentially(title, { delay: 55 });
    await page.waitForTimeout(250);
    await click(page, page.getByRole("button", { name: "Add task" }));
  };
  const column = (page, status) => page.locator(`[data-status="${status}"]`);
  const card = (page, title, status) =>
    (status ? column(page, status) : page).locator("[data-task-id]", { hasText: title });
  // Drag a card into another column, the way a person would.
  const drag = async (page, title, status) => {
    await pointAt(page, card(page, title));
    await page.mouse.down();
    const box = await column(page, status).boundingBox();
    await page.mouse.move(box.x + box.width / 2, box.y + box.height - 24, { steps: 24 });
    await page.mouse.up();
    await card(page, title, status).waitFor();
  };
  const sync = (page) => page.getByRole("switch", { name: "Sync" });

  await caption("Two crew members, one live show board", 2000);
  await caption("Mia adds a task…", 400);
  await add(a, "Tape down the cable runs");
  await card(b, "Tape down the cable runs", "todo").waitFor();
  await caption("…and it appears on Cole's board right away", 2000);

  await caption("Cole starts on soundcheck…", 400);
  await drag(b, "Soundcheck with the band", "doing");
  await card(a, "Soundcheck with the band", "doing").waitFor();
  await caption("…and Mia's board follows", 2000);

  await caption("Mia turns Sync off and keeps working", 400);
  await click(a, sync(a));
  await a.getByRole("switch", { name: "Sync", checked: false }).waitFor();
  await add(a, "Top up the hazer fluid");
  await drag(a, "Print setlists and tape them down", "doing");
  await caption("Her edits apply locally. Cole doesn't have them yet", 2600);
  if (
    (await card(b, "Top up the hazer fluid").count()) ||
    (await card(b, "Print setlists and tape them down", "doing").count())
  )
    throw new Error("Offline edits reached Cole before Mia reconnected");

  await caption("Sync back on…", 400);
  await click(a, sync(a));
  await card(b, "Top up the hazer fluid", "todo").waitFor();
  await card(b, "Print setlists and tape them down", "doing").waitFor();
  await caption("…and Cole's board catches up", 2200);
  await caption("Every change: local first, then synced", 2000);
  const end = Date.now();
  captions.at(-1).to = end - origin;

  const recorded = [];
  for (const device of devices) {
    const video = device.page.video();
    await device.context.close();
    recorded.push({ path: await video.path(), label: device.label, startedAt: device.startedAt });
  }
  await mkdir(outDir, { recursive: true });
  const encoded = await composeWindows({
    browser,
    devices: recorded,
    address: "stageplan.example.com",
    workDir: recordingDir,
    captions,
    origin,
    end,
    outDir,
    id,
    size,
  });
  console.log(
    `Wrote ${encoded.mp4} (${(encoded.bytes / 1e6).toFixed(2)} MB, crf ${encoded.crf}) and ${encoded.poster}`,
  );
} catch (error) {
  console.error(example.log());
  throw error;
} finally {
  await browser?.close();
  example.stop();
  await rm(recordingDir, { recursive: true, force: true });
}
