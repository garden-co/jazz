// EpicDrop (React + Vite, local-first account): first visit, folder reload,
// edit link join, invitee reload, a 3 MB upload, live upload.
import zlib from "node:zlib";
import { device, random, visible } from "../harness.mjs";

const file = (page, name) => page.getByRole("button", { name, exact: true });
const uploads = (page) => page.getByRole("list", { name: "Uploads" });
const input = (page) => page.locator('input[type="file"]').first();
const join = (page) => page.getByRole("button", { name: "Join folder" });

export default function epicDrop({ server, browser }) {
  const url = `${server.origin}/`;
  const devices = new Set();
  const open = async () => {
    const d = await device(browser);
    devices.add(d);
    return d;
  };
  const close = async (d) => {
    devices.delete(d);
    await d?.close();
  };
  const empty = (page) => visible(page.getByText("No folders yet"), { timeout: 120_000 });
  const createFolder = async (page, name) => {
    await page.getByRole("button", { name: "New folder" }).first().click();
    await page.getByLabel("Folder name").fill(name);
    await page.getByRole("button", { name: "Create" }).click();
    await visible(page.getByRole("heading", { name }));
  };
  const folder = `Tour demos ${random()}`;
  let owner, invitee, link;

  return {
    async setup() {
      owner = await open();
      const { page } = owner;
      await page.goto(url);
      await empty(page);
      await createFolder(page, folder);
      await uploadDemos(page);
      await page.getByRole("button", { name: "Share" }).first().click();
      await page.getByText("Can edit", { exact: true }).click();
      await page.getByRole("button", { name: "Create link" }).click();
      const box = page.getByLabel("Invite link");
      await box.and(page.locator(":not([value=''])")).waitFor();
      link = await box.inputValue();
      await page.keyboard.press("Escape");
    },
    scenarios: {
      cold: {
        prepare: async () => ({ d: await open() }),
        async run({ d }) {
          await d.page.goto(url);
          await empty(d.page);
        },
        finish: ({ d }) => close(d),
      },
      reload: {
        async run() {
          await owner.page.reload();
          await visible(file(owner.page, "set-list.txt"));
        },
      },
      invite_join: {
        // A visitor (with their own local-first account) on the edit link,
        // with the join button enabled.
        async prepare() {
          const d = await open();
          await d.page.goto(url);
          await empty(d.page);
          await d.page.goto(link);
          await join(d.page)
            .and(d.page.locator(":not([disabled]):not([aria-disabled='true'])"))
            .waitFor();
          return { d };
        },
        async run({ d }) {
          await join(d.page).click();
          await visible(file(d.page, "set-list.txt"));
        },
        // The last member to join is the invitee of the cases below.
        async finish({ d }) {
          await close(invitee);
          invitee = d;
        },
      },
      invitee_reload: {
        async run() {
          await invitee.page.reload();
          await visible(file(invitee.page, "set-list.txt"));
        },
      },
      upload_3mb: {
        // Each upload goes into a new visitor's new folder, so every
        // iteration starts from an empty upload queue and an empty folder.
        async prepare() {
          const d = await open();
          await d.page.goto(url);
          await empty(d.page);
          await createFolder(d.page, `Stems ${random()}`);
          return { d };
        },
        async run({ d }) {
          await uploadDemos(d.page);
        },
        finish: ({ d }) => close(d),
      },
      live_upload: {
        prepare: () => ({ name: `idea-${random()}.txt` }),
        async run({ name }) {
          await input(owner.page).setInputFiles([
            { name, mimeType: "text/plain", buffer: Buffer.from("Open with the slow version.\n") },
          ]);
          await visible(file(invitee.page, name));
        },
      },
    },
    async close() {
      for (const d of devices) await d.close();
    },
  };
}

/**
 * The walkthrough's upload: a long set list, a cover image and 3 MB of stems.
 * Returns once the upload queue has drained (a reload during an upload drops
 * it).
 */
async function uploadDemos(page) {
  const setList = Array.from({ length: 4000 }, (_, i) => `${i + 1}. Song number ${i + 1}`);
  await input(page).setInputFiles([
    { name: "set-list.txt", mimeType: "text/plain", buffer: Buffer.from(setList.join("\n")) },
    { name: "cover.png", mimeType: "image/png", buffer: png(800, 500) },
    {
      name: "stems.bin",
      mimeType: "application/octet-stream",
      buffer: Buffer.alloc(3 * 1024 * 1024, 0x2a),
    },
  ]);
  await visible(file(page, "stems.bin"));
  await uploads(page).waitFor({ state: "detached", timeout: 180_000 });
}

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
