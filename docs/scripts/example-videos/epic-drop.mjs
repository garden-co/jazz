// Examples-page walkthrough for EpicDrop (examples/epic-drop, React + Vite).
//   node scripts/example-videos/epic-drop.mjs   # writes public/examples/videos/epic-drop.*
import { rm } from "node:fs/promises";
import { join } from "node:path";
import zlib from "node:zlib";
import { record } from "./walkthrough.mjs";
import { click, pointAt, sleep, type } from "./stage.mjs";

const port = 5287;
const url = `http://127.0.0.1:${port}/`;

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

await record({
  id: "epic-drop",
  app: "examples/epic-drop",
  server: ({ dir }) => ({
    before: () =>
      rm(join(dir, "node_modules/.cache/jazz-dev-server"), { recursive: true, force: true }),
    command: "pnpm",
    args: ["exec", "vite", "--port", String(port), "--strictPort", "--host", "127.0.0.1"],
    ready: /Local:\s+http/,
  }),
  async run(stage) {
    const device = { address: "epicdrop.example.com", keepPorts: [port] };
    const a = await stage.device("a", { ...device, name: "Alice's laptop" });
    const b = await stage.device("b", { ...device, name: "Bob's laptop", color: "#ea580c" });
    const file = (page, name) => page.getByRole("button", { name, exact: true });
    const upload = async (page, files) => {
      await pointAt(page, page.getByText("Drop files here or browse").first());
      await page.locator('input[type="file"]').first().setInputFiles(files);
    };

    await a.goto(url);
    await a.getByText("No folders yet").last().waitFor({ timeout: 120_000 });
    await b.goto(url);
    await b.getByText("No folders yet").last().waitFor({ timeout: 120_000 });

    await stage.start();
    await stage.full("a");
    stage.roll();
    await stage.title(
      "EpicDrop",
      "A file browser for a band's demos, built on Jazz large values. React + Jazz.",
      2400,
    );
    await stage.caption("Each browser is an anonymous local-first Jazz account", 1500);
    await click(a, a.getByRole("button", { name: "New folder" }).first());
    await type(a, a.getByLabel("Folder name"), "Tour demos", { delay: 80 });
    await click(a, a.getByRole("button", { name: "Create" }));
    await a.getByRole("heading", { name: "Tour demos" }).waitFor();
    await sleep(600);

    await stage.caption("Uploads stream File.stream() into a Jazz bytes column, chunk by chunk");
    const setList = Array.from({ length: 4000 }, (_, i) => `${i + 1}. Song number ${i + 1}`).join(
      "\n",
    );
    await upload(a, [
      { name: "set-list.txt", mimeType: "text/plain", buffer: Buffer.from(setList) },
      { name: "cover.png", mimeType: "image/png", buffer: png(800, 500) },
      {
        name: "stems.bin",
        mimeType: "application/octet-stream",
        buffer: Buffer.alloc(3 * 1024 * 1024, 0x2a),
      },
    ]);
    await file(a, "stems.bin").waitFor();
    await sleep(1500);

    await stage.caption(
      "Previews read only the byte range they show: select({ contents: { from, to } })",
    );
    await click(a, file(a, "set-list.txt"));
    await a.getByRole("button", { name: "Show more" }).waitFor();
    await sleep(1400);
    await click(a, a.getByRole("button", { name: "Show more" }), { after: 1400 });
    await stage.caption("Images, audio, video and PDFs load the whole value into a Blob");
    await click(a, file(a, "cover.png"), { after: 2000 });
    await stage.caption("Anything else: a hex dump of the first 512 bytes");
    await click(a, file(a, "stems.bin"), { after: 2200 });
    await click(a, a.getByRole("button", { name: "Close preview" }));

    await stage.caption("Sharing: an invite link, checked by a permission at the sync server");
    await click(a, a.getByRole("button", { name: "Share" }).first());
    await click(a, a.getByText("Can edit", { exact: true }));
    await click(a, a.getByRole("button", { name: "Create link" }));
    const linkInput = a.getByLabel("Invite link");
    await linkInput.and(a.locator(":not([value=''])")).waitFor();
    const link = await linkInput.inputValue();
    await sleep(1600);
    await a.keyboard.press("Escape");
    await sleep(400);

    await stage.split("a", "b", { scale: 0.6 });
    await stage.caption("Bob opens the link on his laptop");
    await b.goto(link);
    await stage.recast("b");
    await click(b, b.getByRole("button", { name: "Join folder" }), { after: 300 });
    await b.getByRole("heading", { name: "Tour demos" }).waitFor();
    await file(b, "stems.bin").waitFor({ timeout: 60_000 });
    await stage.caption("Bob now sees the folder and its files, with edit access", 2400);

    await stage.caption("Alice's open folder is a live query for its files…");
    await upload(b, [
      {
        name: "bob-idea.txt",
        mimeType: "text/plain",
        buffer: Buffer.from("Open with the slow version of track 3.\n"),
      },
    ]);
    await file(a, "bob-idea.txt").waitFor();
    stage.poster();
    await stage.caption("…so Bob's upload appears for Alice without a reload", 2400);

    await stage.caption("Bob's Wi-Fi drops…");
    await stage.wifi("b", false);
    await stage.caption("…while Alice renames a file");
    await click(a, a.getByRole("button", { name: "Actions for set-list.txt" }));
    await click(a, a.getByRole("menuitem", { name: "Rename" }));
    await type(a, a.getByLabel("Name", { exact: true }), "set-list-final.txt", {
      clear: true,
      delay: 60,
    });
    await click(a, a.getByRole("button", { name: "Rename", exact: true }));
    await sleep(1500);
    if (await file(b, "set-list-final.txt").count())
      throw new Error("The rename reached Bob while his Wi-Fi was off");
    await stage.caption("Bob still has the old name", 1800);
    await stage.caption("Wi-Fi back on…");
    await stage.wifi("b", true);
    await file(b, "set-list-final.txt").waitFor({ timeout: 30_000 });
    await stage.caption("…and the new name syncs in", 2200);

    await stage.caption("Bob reloads: his copy is stored locally and syncs back in");
    await b.reload();
    await stage.recast("b");
    await file(b, "set-list-final.txt").waitFor({ timeout: 60_000 });
    await sleep(1800);
    await stage.caption("");
  },
});
