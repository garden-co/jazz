// Uploads encoded walkthroughs from docs/.example-videos/ to Vercel Blob and
// records their public URLs in lib/showcase/videos.json, which the examples
// page reads. Paths are content-addressed, so a re-record never overwrites a
// URL a deployed page still uses.
//
//   BLOB_READ_WRITE_TOKEN=… pnpm --filter docs upload:example-videos
import { createHash } from "node:crypto";
import { readdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { put } from "@vercel/blob";
import { MAX_BYTES } from "./encode.mjs";

const dir = fileURLToPath(new URL("../../.example-videos/", import.meta.url));
const manifestPath = fileURLToPath(new URL("../../lib/showcase/videos.json", import.meta.url));
if (!process.env.BLOB_READ_WRITE_TOKEN) throw new Error("BLOB_READ_WRITE_TOKEN is not set.");

const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
const upload = async (file, contentType) => {
  const body = await readFile(`${dir}${file}`);
  const hash = createHash("sha256").update(body).digest("hex").slice(0, 16);
  const [name, extension] = file.split(/\.(?=[^.]+$)/);
  const blob = await put(`examples/videos/${name}-${hash}.${extension}`, body, {
    access: "public",
    contentType,
    addRandomSuffix: false,
    allowOverwrite: true,
    cacheControlMaxAge: 31536000,
  });
  return { url: blob.url, bytes: body.length };
};

for (const file of (await readdir(dir)).filter((f) => f.endsWith(".mp4"))) {
  const id = file.slice(0, -4);
  const video = await upload(file, "video/mp4");
  if (video.bytes > MAX_BYTES) throw new Error(`${file} is over the ${MAX_BYTES}-byte budget`);
  const poster = await upload(`${id}.jpg`, "image/jpeg");
  manifest[id] = { mp4: video.url, poster: poster.url, bytes: video.bytes };
  console.log(`${id}: ${video.url}`);
}
await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
