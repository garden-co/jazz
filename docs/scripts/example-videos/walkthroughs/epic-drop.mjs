// EpicDrop (examples/epic-drop, React + Vite): server and actions for
// epic-drop.storyboard.ts.
import zlib from "node:zlib";
import { click, pointAt, sleep, type } from "../stage.mjs";
import { viteServer } from "../walkthrough.mjs";

const port = 5287;
const url = `http://127.0.0.1:${port}/`;

export const app = "examples/epic-drop";
export const server = ({ dir }) => viteServer({ dir, port });
export const deviceOptions = { keepPorts: [port] };

const file = (page, name) => page.getByRole("button", { name, exact: true });
const upload = async (page, files) => {
  await pointAt(page, page.getByText("Drop files here or browse").first());
  await page.locator('input[type="file"]').first().setInputFiles(files);
};

export const actions = {
  async openApp({ page }) {
    await page.goto(url);
    await page.getByText("No folders yet").last().waitFor({ timeout: 120_000 });
  },
  async createFolder({ page }, name) {
    await click(page, page.getByRole("button", { name: "New folder" }).first());
    await type(page, page.getByLabel("Folder name"), name, { delay: 80 });
    await click(page, page.getByRole("button", { name: "Create" }));
    await page.getByRole("heading", { name }).waitFor();
  },
  /** A long set list, a cover image and 3 MB of stems. */
  async uploadDemos({ page }) {
    const setList = Array.from({ length: 4000 }, (_, i) => `${i + 1}. Song number ${i + 1}`).join(
      "\n",
    );
    await upload(page, [
      { name: "set-list.txt", mimeType: "text/plain", buffer: Buffer.from(setList) },
      { name: "cover.png", mimeType: "image/png", buffer: png(800, 500) },
      {
        name: "stems.bin",
        mimeType: "application/octet-stream",
        buffer: Buffer.alloc(3 * 1024 * 1024, 0x2a),
      },
    ]);
    await file(page, "stems.bin").waitFor();
  },
  /** Opens a text preview, then reads more of it. */
  async previewText({ page }, name) {
    await click(page, file(page, name));
    await page.getByRole("button", { name: "Show more" }).waitFor();
    await sleep(1400);
    await click(page, page.getByRole("button", { name: "Show more" }), { after: 1400 });
  },
  async preview({ page }, name, { after = 400 } = {}) {
    await click(page, file(page, name), { after });
  },
  async closePreview({ page }) {
    await click(page, page.getByRole("button", { name: "Close preview" }));
  },
  async createEditLink({ page, state }) {
    await click(page, page.getByRole("button", { name: "Share" }).first());
    await click(page, page.getByText("Can edit", { exact: true }));
    await click(page, page.getByRole("button", { name: "Create link" }));
    const linkInput = page.getByLabel("Invite link");
    await linkInput.and(page.locator(":not([value=''])")).waitFor();
    state.link = await linkInput.inputValue();
  },
  async press({ page }, key) {
    await page.keyboard.press(key);
  },
  async joinFolder({ stage, on, page, state }, name) {
    await page.goto(state.link);
    await stage.recast(on);
    await click(page, page.getByRole("button", { name: "Join folder" }), { after: 300 });
    await page.getByRole("heading", { name }).waitFor();
    await file(page, "stems.bin").waitFor({ timeout: 60_000 });
  },
  async uploadIdea({ page }) {
    await upload(page, [
      {
        name: "bob-idea.txt",
        mimeType: "text/plain",
        buffer: Buffer.from("Open with the slow version of track 3.\n"),
      },
    ]);
  },
  async waitForFile({ page }, name, { timeout = 30_000 } = {}) {
    await file(page, name).waitFor({ timeout });
  },
  async expectNoFile({ page }, name) {
    if (await file(page, name).count())
      throw new Error(`"${name}" reached this laptop while its Wi-Fi was off`);
  },
  async rename({ page }, from, to) {
    await click(page, page.getByRole("button", { name: `Actions for ${from}` }));
    await click(page, page.getByRole("menuitem", { name: "Rename" }));
    await type(page, page.getByLabel("Name", { exact: true }), to, { clear: true, delay: 60 });
    await click(page, page.getByRole("button", { name: "Rename", exact: true }));
  },
  async reload({ stage, on, page }) {
    await page.reload();
    await stage.recast(on);
  },
};

// A small gradient PNG to upload.
function png(w, h) {
  const table = Array.from({ length: 256 }, (_, n) => {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    return c >>> 0;
  });
  const crc = (buf) => {
    let c = 0xffffffff;
    for (const b of buf) c = table[(c ^ b) & 0xff] ^ (c >>> 8);
    return (c ^ 0xffffffff) >>> 0;
  };
  const chunk = (type, data) => {
    const len = Buffer.alloc(4);
    len.writeUInt32BE(data.length);
    const body = Buffer.concat([Buffer.from(type), data]);
    const sum = Buffer.alloc(4);
    sum.writeUInt32BE(crc(body));
    return Buffer.concat([len, body, sum]);
  };
  const header = Buffer.alloc(13);
  header.writeUInt32BE(w, 0);
  header.writeUInt32BE(h, 4);
  header[8] = 8;
  header[9] = 2;
  const raw = Buffer.alloc((w * 3 + 1) * h);
  for (let y = 0; y < h; y++)
    for (let x = 0; x < w; x++) {
      const o = y * (w * 3 + 1) + 1 + x * 3;
      raw[o] = (40 + (x * 180) / w) | 0;
      raw[o + 1] = (60 + (y * 150) / h) | 0;
      raw[o + 2] = 200;
    }
  return Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk("IHDR", header),
    chunk("IDAT", zlib.deflateSync(raw)),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}
