// Records the Todos hero walkthrough: two independent "devices" side by side,
// syncing through the local Jazz server that the example's Vite plugin starts.
//
//   pnpm build:core                              # jazz-tools, WASM and NAPI
//   pnpm --filter docs capture:example-videos    # writes docs/public/examples/videos/
//
// The devices are two iframes on different origins (localhost and 127.0.0.1),
// so each has its own IndexedDB, local-first identity and Jazz client. Nothing
// is mocked: every change travels device → server → device.
import { spawn } from "node:child_process";
import { mkdir, rename, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const exampleDir = fileURLToPath(
  new URL("../../../examples/todo-client-localfirst-react/", import.meta.url),
);
const outDir = fileURLToPath(new URL("../../public/examples/videos/", import.meta.url));
const port = Number(process.env.EXAMPLE_PORT ?? 5199);
const size = { width: 1280, height: 720 };
const devices = [
  { name: "Device A", url: `http://localhost:${port}/` },
  { name: "Device B", url: `http://127.0.0.1:${port}/` },
];

function startExample() {
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

const harness = `<!doctype html>
<html><head><style>
  body { margin: 0; height: 100vh; display: flex; flex-direction: column; background: #0f1115;
         font: 15px/1.4 ui-sans-serif, system-ui, sans-serif; color: #e7e9ee; }
  #caption { height: 64px; display: flex; align-items: center; justify-content: center;
             font-size: 20px; letter-spacing: -0.2px; transition: opacity .2s; }
  main { flex: 1; display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 0 16px 16px; }
  section { display: flex; flex-direction: column; border-radius: 12px; overflow: hidden; background: #fff; }
  header { padding: 8px 12px; background: #1c1f26; font-size: 13px; color: #aab0bd;
           display: flex; justify-content: space-between; }
  iframe { border: 0; flex: 1; width: 100%; }
</style></head><body>
  <div id="caption">Two devices, one live todo list</div>
  <main>${devices
    .map(
      (d, i) =>
        `<section><header><strong style="color:#e7e9ee">${d.name}</strong><span>${new URL(d.url).host}</span></header><iframe name="device${i}" src="${d.url}"></iframe></section>`,
    )
    .join("")}</main>
</body></html>`;

const example = startExample();
let browser;
try {
  await example.ready;
  browser = await chromium.launch({
    executablePath: process.env.CHROMIUM_PATH || undefined,
    // Both hostnames reach the same local server; they stay separate origins.
    args: ["--host-resolver-rules=MAP localhost 127.0.0.1"],
  });
  await rm(`${outDir}.recording`, { recursive: true, force: true });
  const context = await browser.newContext({
    viewport: size,
    recordVideo: { dir: `${outDir}.recording`, size },
  });
  const page = await context.newPage();
  await page.setContent(harness);
  const [a, b] = [0, 1].map((i) => page.frameLocator(`iframe[name=device${i}]`));
  const caption = async (text, pause = 900) => {
    await page.locator("#caption").evaluate((el, t) => (el.textContent = t), text);
    await page.waitForTimeout(pause);
  };
  const add = async (device, title) => {
    await device.getByPlaceholder("What needs to be done?").pressSequentially(title, { delay: 55 });
    await page.waitForTimeout(250);
    await device.getByRole("button", { name: "Add" }).click();
  };
  const todo = (device, title) => device.locator("#todo-list li", { hasText: title });

  for (const device of [a, b]) {
    await device
      .getByRole("button", { name: "Add" })
      .and(device.locator(":enabled"))
      .waitFor({ timeout: 60_000 });
  }
  await caption("Two devices, one live todo list", 1500);

  await caption("Device A adds a todo…", 400);
  await add(a, "Buy oat milk");
  await todo(b, "Buy oat milk").waitFor();
  await caption("…and it appears on Device B right away", 1600);

  await caption("Device B adds one too", 400);
  await add(b, "Book the rehearsal room");
  await todo(a, "Book the rehearsal room").waitFor();
  await page.waitForTimeout(1200);

  await caption("Device A checks off its own todo", 500);
  await todo(a, "Buy oat milk").locator("input.toggle").check();
  await todo(b, "Buy oat milk").locator("input.toggle:checked").waitFor();
  await caption("The change syncs to Device B", 1600);

  await caption("Device B tries to change a todo it doesn't own…", 600);
  await todo(b, "Book the rehearsal room").locator("input.toggle").check();
  await todo(b, "Buy oat milk").locator("input.toggle").click();
  await b.getByText("You don't have permission", { exact: false }).waitFor();
  await caption("…and the permission policy refuses it", 2200);

  await caption("Filters are live queries", 400);
  await a.getByLabel("Filter by title").pressSequentially("room", { delay: 90 });
  await page.waitForTimeout(1400);
  await a.getByLabel("Filter by title").fill("");
  await caption("Every change: local first, then synced", 1500);
  await page.screenshot({ path: `${outDir}todo-two-devices.png` });

  const video = page.video();
  await context.close();
  await mkdir(outDir, { recursive: true });
  await rename(await video.path(), `${outDir}todo-two-devices.webm`);
  await rm(`${outDir}.recording`, { recursive: true, force: true });
  console.log(`Wrote ${outDir}todo-two-devices.webm`);
} catch (error) {
  console.error(example.log());
  throw error;
} finally {
  await browser?.close();
  example.stop();
}
