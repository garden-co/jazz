// Records the homepage walkthrough: StagePlan on two independent devices side
// by side, a crew chief and a crew member on the same show board, syncing
// through the local Jazz server that the example's Vite plugin starts.
//
//   pnpm build:core                              # jazz-tools, WASM and NAPI
//   pnpm --filter docs capture:example-videos    # writes docs/public/examples/videos/
//
// Nothing is mocked: each device is its own browser context (own IndexedDB,
// local-first identity and Jazz client) and every change travels device →
// server → device. "Wi-Fi off" in a device's menu bar really cuts that
// device's connection to the sync server (see ./stage.mjs). Only Vite's own
// dev server stays reachable, so its hot-reload client doesn't reload the page.

import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { encodeRecording, startServer } from "./encode.mjs";
import { Stage, click, pointAt, sleep, type } from "./stage.mjs";

const exampleDir = fileURLToPath(
  new URL("../../../examples/stage-plan/apps/react-localfirst/", import.meta.url),
);
const outDir = fileURLToPath(new URL("../../public/examples/videos/", import.meta.url));
const id = "stage-plan-two-devices";
const port = Number(process.env.EXAMPLE_PORT ?? 5199);
const url = `http://127.0.0.1:${port}/`;
const SHOW = "The Late Lanterns: album launch";

// Start from an empty sync server so each recording starts from a fresh demo show.
await rm(join(exampleDir, "node_modules/.cache/jazz-dev-server"), { recursive: true, force: true });
const server = await startServer(
  "pnpm",
  ["exec", "vite", "--port", String(port), "--strictPort", "--host", "127.0.0.1"],
  { cwd: exampleDir, ready: /Local:\s+http/ },
);
const videoDir = await mkdtemp(join(tmpdir(), "example-video-"));
// Large subtitles: on the homepage the clip plays at about half size.
const stage = await Stage.launch({ width: 1280, height: 800, captionSize: 30, videoDir });
try {
  const a = await stage.device("a", {
    name: "Mia's laptop",
    address: "stageplan.example.com",
    color: "#2563eb",
    colorScheme: "dark",
    keepPorts: [port],
  });
  const b = await stage.device("b", {
    name: "Cole's laptop",
    address: "stageplan.example.com",
    color: "#ea580c",
    colorScheme: "dark",
    keepPorts: [port],
  });

  // Off camera: Mia's first run creates the demo show, and Cole joins it
  // through Mia's invite link.
  const rename = async (page, name) => {
    await page.getByRole("button", { name: /^Stagehand / }).click();
    await page.getByRole("textbox", { name: "Name" }).fill(name);
    await page.getByRole("button", { name: "Save" }).click();
    await page.getByRole("button", { name }).waitFor();
  };
  await a.goto(url);
  const showLink = a.getByRole("link", { name: SHOW });
  await showLink.waitFor({ timeout: 90_000 });
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

  await stage.start();
  await stage.split("a", "b", { scale: 0.8 });
  for (const page of [a, b])
    await page.getByRole("link", { name: "Soundcheck with the band" }).waitFor();
  await sleep(1200);
  stage.roll();

  const column = (page, status) => page.locator(`[data-status="${status}"]`);
  const card = (page, title, status) =>
    (status ? column(page, status) : page).locator("[data-task-id]", { hasText: title });
  const add = async (page, title) => {
    await type(page, page.getByRole("textbox", { name: "New task" }), title);
    await click(page, page.getByRole("button", { name: "Add task" }), { after: 200 });
  };
  // Drag a card into another column, the way a person would.
  const drag = async (page, title, status) => {
    await pointAt(page, card(page, title));
    await page.mouse.down();
    const box = await column(page, status).boundingBox();
    await page.mouse.move(box.x + box.width / 2, box.y + box.height - 24, { steps: 24 });
    await page.mouse.up();
    await card(page, title, status).waitFor();
  };
  const comment = async (page, text) => {
    await type(page, page.getByPlaceholder("Add a comment for the crew"), text);
    await click(page, page.getByRole("button", { name: "Comment" }), { after: 200 });
  };
  const closeDialog = async (page) => {
    await page.keyboard.press("Escape");
    await page.getByPlaceholder("Add a comment for the crew").waitFor({ state: "hidden" });
  };

  await stage.caption("Two crew members, one live show board", 2200);

  await stage.caption("Mia adds a task…");
  await add(a, "Tape down the cable runs");
  await card(b, "Tape down the cable runs", "todo").waitFor();
  await stage.caption("…and it appears on Cole's board right away", 2200);

  await stage.caption("Cole starts on soundcheck…");
  await drag(b, "Soundcheck with the band", "doing");
  await card(a, "Soundcheck with the band", "doing").waitFor();
  await stage.caption("…and Mia's board follows", 2000);

  // Live query: Mia's open task shows only that task's comments, and they update live.
  await stage.caption("Mia opens soundcheck: a live query for just its comments");
  await click(a, card(a, "Soundcheck with the band"), { after: 300 });
  await a.getByPlaceholder("Add a comment for the crew").waitFor();
  await click(b, card(b, "Soundcheck with the band"), { after: 300 });
  await stage.caption("Cole comments on that task…");
  await comment(b, "Drums are miked");
  await a.getByText("Drums are miked").waitFor();
  await stage.caption("…and it shows up in Mia's open task immediately", 2600);
  await closeDialog(b);
  await closeDialog(a);

  await stage.caption("Mia turns off her laptop's Wi-Fi");
  await stage.wifi("a", false);
  await stage.caption("She keeps working: edits apply locally");
  await add(a, "Top up the hazer fluid");
  await drag(a, "Print setlists and tape them down", "doing");
  await stage.caption("Cole doesn't have them yet", 2400);
  if (
    (await card(b, "Top up the hazer fluid").count()) ||
    (await card(b, "Print setlists and tape them down", "doing").count())
  )
    throw new Error("Offline edits reached Cole while Mia's Wi-Fi was off");

  await stage.caption("Wi-Fi back on…");
  await stage.wifi("a", true);
  await card(b, "Top up the hazer fluid", "todo").waitFor({ timeout: 30_000 });
  await card(b, "Print setlists and tape them down", "doing").waitFor({ timeout: 30_000 });
  await stage.caption("…and Cole's board catches up", 2400);
  await stage.caption("Every change: local first, then synced", 2400);

  const { path, trimStart } = await stage.finish();
  await mkdir(outDir, { recursive: true });
  const encoded = await encodeRecording({ input: path, trimStart, outDir, id });
  console.log(
    `Wrote ${encoded.mp4} (${(encoded.bytes / 1e6).toFixed(2)} MB, crf ${encoded.crf}) and ${encoded.poster}`,
  );
} catch (error) {
  console.error(server.log());
  await stage.abort(process.env.DEBUG_DIR);
  throw error;
} finally {
  await server.stop();
  await stage.cleanup();
}
