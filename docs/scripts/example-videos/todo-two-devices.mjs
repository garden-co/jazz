// Records the Todos hero walkthrough: two independent "devices" side by side,
// syncing through the local Jazz server that the example's Vite plugin starts.
//
//   pnpm build:core                              # jazz-tools, WASM and NAPI
//   pnpm --filter docs capture:example-videos    # records and encodes into docs/.example-videos/
//   pnpm --filter docs upload:example-videos     # uploads to Vercel Blob, updates the manifest
//
// Each device is its own browser context, so it has its own IndexedDB,
// local-first identity and Jazz client. Nothing is mocked: every change
// travels device → server → device. ffmpeg composes the two recordings.

import { spawn } from "node:child_process";
import { mkdir, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { composeSideBySide } from "./encode.mjs";

const exampleDir = fileURLToPath(
  new URL("../../../examples/todo-client-localfirst-react/", import.meta.url),
);
const outDir = fileURLToPath(new URL("../../.example-videos/", import.meta.url));
const id = "todo-two-devices";
const port = Number(process.env.EXAMPLE_PORT ?? 5199);
const size = { width: 600, height: 560 };
const url = `http://127.0.0.1:${port}/`;

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
let browser;
try {
  await example.ready;
  browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined });
  await rm(`${outDir}.recording`, { recursive: true, force: true });
  const devices = [];
  for (const label of ["Device A", "Device B"]) {
    const context = await browser.newContext({
      viewport: size,
      recordVideo: { dir: `${outDir}.recording/${devices.length}`, size },
    });
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
  const add = async (page, title) => {
    await page.getByPlaceholder("What needs to be done?").pressSequentially(title, { delay: 55 });
    await page.waitForTimeout(250);
    await page.getByRole("button", { name: "Add" }).click();
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
  await todo(a, "Buy oat milk").locator("input.toggle").check();
  await todo(b, "Buy oat milk").locator("input.toggle:checked").waitFor();
  await caption("The change syncs to Device B", 1600);

  await caption("Device B tries to uncheck a todo it doesn't own…", 600);
  // The server's authorization rejects the write; wait for that verdict.
  const rejected = b.waitForEvent("console", (m) => m.text().includes("permission_denied"));
  await todo(b, "Buy oat milk").locator("input.toggle").click();
  await rejected;
  await todo(b, "Buy oat milk").locator("input.toggle:checked").waitFor();
  await caption("…the server's permission policy rejects it", 2200);

  await caption("Filters are live queries", 400);
  await a.getByLabel("Filter by title").pressSequentially("room", { delay: 90 });
  await a.waitForTimeout(1400);
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
  const encoded = await composeSideBySide({
    devices: recorded,
    captions,
    origin,
    end,
    outDir,
    id,
    size,
  });
  await rm(`${outDir}.recording`, { recursive: true, force: true });
  console.log(
    `Wrote ${encoded.mp4} (${(encoded.bytes / 1e6).toFixed(2)} MB, crf ${encoded.crf}) and ${encoded.poster}`,
  );
} catch (error) {
  console.error(example.log());
  throw error;
} finally {
  await browser?.close();
  example.stop();
}
