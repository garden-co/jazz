// Records the Todos hero walkthrough: two independent "devices" side by side,
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
import { composeWindows, stageFont } from "./encode.mjs";

const exampleDir = fileURLToPath(
  new URL("../../../examples/todo-client-localfirst-react/", import.meta.url),
);
const outDir = fileURLToPath(new URL("../../public/examples/videos/", import.meta.url));
const id = "todo-two-devices";
const port = Number(process.env.EXAMPLE_PORT ?? 5199);
const size = { width: 600, height: 560 };
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
    addEventListener(
      "mousemove",
      (e) => (cursor.style.transform = `translate(${e.clientX - 2}px,${e.clientY - 2}px)`),
      true,
    );
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

// Render the app in the docs site's font, served to the page by a route.
function installFont({ family }) {
  const style = document.createElement("style");
  style.textContent = [400, 700]
    .map(
      (w) =>
        `@font-face{font-family:${family};font-weight:${w};src:url(/__video-font/${w}.woff2) format("woff2")}`,
    )
    .concat(`body,input,button{font-family:${family},system-ui,sans-serif}`)
    .join("");
  document.documentElement.append(style);
}

async function startExample() {
  // Start from an empty sync server so each recording shows only its own todos.
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

const example = await startExample();
const recordingDir = await mkdtemp(join(tmpdir(), "example-video-"));
let browser;
try {
  await example.ready;
  browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined });
  const devices = [];
  for (const [label, color] of [
    ["Device A", "#2563eb"],
    ["Device B", "#ea580c"],
  ]) {
    const context = await browser.newContext({
      viewport: size,
      colorScheme: "dark",
      recordVideo: { dir: join(recordingDir, String(devices.length)), size },
    });
    await context.addInitScript(installCursor, { color, glideMs });
    await context.addInitScript(installFont, { family: stageFont.family });
    await context.route("**/__video-font/*.woff2", (route) =>
      route.fulfill({
        path: fileURLToPath(
          stageFont.files[new URL(route.request().url()).pathname.match(/(\d+)\.woff2$/)[1]],
        ),
        contentType: "font/woff2",
      }),
    );
    const page = await context.newPage();
    devices.push({ label, context, page, startedAt: Date.now() });
    await page.goto(url);
  }
  const [a, b] = devices.map((d) => d.page);
  for (const page of [a, b]) {
    await page
      .getByRole("button", { name: "Add" })
      .and(page.locator(":enabled"))
      .waitFor({ timeout: 60_000 });
  }
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
  const type = async (page, locator, text, delay) => {
    await click(page, locator);
    await locator.pressSequentially(text, { delay });
  };
  const add = async (page, title) => {
    await type(page, page.getByPlaceholder("What needs to be done?"), title, 55);
    await page.waitForTimeout(250);
    await click(page, page.getByRole("button", { name: "Add" }));
  };
  const todo = (page, title) => page.locator("#todo-list li", { hasText: title });

  await caption("Two devices, one live todo list", 1500);
  await caption("Device A adds a todo…", 400);
  await add(a, "Buy oat milk");
  await todo(b, "Buy oat milk").waitFor();
  await caption("…and it appears on Device B right away", 1600);

  await caption("Device B adds one too", 400);
  await add(b, "Book the rehearsal room");
  await todo(a, "Book the rehearsal room").waitFor();
  await a.waitForTimeout(1200);

  await caption("Device A checks off its own todo", 500);
  await click(a, todo(a, "Buy oat milk").locator("input.toggle"));
  await todo(b, "Buy oat milk").locator("input.toggle:checked").waitFor();
  await caption("The change syncs to Device B", 1600);

  await caption("Device B tries to uncheck a todo it doesn't own…", 600);
  // The server's authorization rejects the write; wait for that verdict.
  const rejected = b.waitForEvent("console", (m) => m.text().includes("permission_denied"));
  await click(b, todo(b, "Buy oat milk").locator("input.toggle"));
  await rejected;
  await todo(b, "Buy oat milk").locator("input.toggle:checked").waitFor();
  await caption("…the server's permission policy rejects it", 2200);

  await caption("Device A filters its list", 400);
  await type(a, a.getByLabel("Filter by title"), "room", 90);
  await a.waitForTimeout(900);
  await caption("Device B adds a matching todo…", 400);
  await add(b, "Tidy up the practice room");
  await todo(a, "Tidy up the practice room").waitFor();
  await caption("…and A's filtered view updates by itself: filters are live queries", 2400);
  await click(a, a.getByLabel("Filter by title"));
  await a.getByLabel("Filter by title").fill("");
  await caption("Every change: local first, then synced", 1800);
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
    address: `localhost:${port}`,
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
