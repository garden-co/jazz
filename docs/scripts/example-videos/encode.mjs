// Composes per-device Playwright recordings into one streaming-friendly H.264
// MP4 (plays in every browser, including Safari) plus a JPEG poster, under a
// size budget. Needs ffmpeg with libx264 and drawtext (freetype).
import { execFile } from "node:child_process";
import { stat } from "node:fs/promises";
import { promisify } from "node:util";

const run = promisify(execFile);
export const MAX_BYTES = 5_000_000;
const font = process.env.EXAMPLE_VIDEO_FONT ?? "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

// drawtext's own mini-language: escape its separators inside text values.
const text = (value) =>
  value.replace(/\\/g, "\\\\").replace(/'/g, "\u2019").replace(/:/g, "\\:").replace(/%/g, "\\%");

/**
 * devices: [{ path, label, startedAt }] recorded side by side; captions:
 * [{ text, from, to }] in ms since `origin` (the moment every device was ready).
 */
export async function composeSideBySide({ devices, captions, origin, end, outDir, id, size }) {
  const mp4 = `${outDir}${id}.mp4`;
  const poster = `${outDir}${id}.jpg`;
  const bar = 72;
  const gap = 16;
  const width = size.width * devices.length + gap * (devices.length + 1);
  const height = size.height + bar + gap + 28;
  const duration = (end - origin) / 1000;
  const inputs = devices.flatMap((d) => [
    "-ss",
    String(Math.max(0, (origin - d.startedAt) / 1000)),
    "-i",
    d.path,
  ]);
  const panels = devices.map(
    (d, i) => `[${i}:v]setpts=PTS-STARTPTS,fps=30,scale=${size.width}:${size.height}[v${i}]`,
  );
  let chain = `color=c=0x0f1115:s=${width}x${height}:r=30:d=${duration}[bg]`;
  let last = "bg";
  devices.forEach((d, i) => {
    const x = gap + i * (size.width + gap);
    chain += `;[${last}][v${i}]overlay=x=${x}:y=${bar + 28}:shortest=1[o${i}]`;
    chain += `;[o${i}]drawtext=fontfile=${font}:text='${text(d.label)}':x=${x + 4}:y=${bar + 4}:fontsize=16:fontcolor=0xaab0bd[l${i}]`;
    last = `l${i}`;
  });
  captions.forEach((c, i) => {
    chain += `;[${last}]drawtext=fontfile=${font}:text='${text(c.text)}':x=(w-text_w)/2:y=(${bar}-text_h)/2+6:fontsize=26:fontcolor=0xe7e9ee:enable='between(t,${(c.from / 1000).toFixed(2)},${(c.to / 1000).toFixed(2)})'[c${i}]`;
    last = `c${i}`;
  });
  const filter = [...panels, chain].join(";");
  // Screen recordings compress well; raise CRF until the clip fits the budget.
  for (const crf of [26, 30, 34, 38]) {
    await run(
      "ffmpeg",
      [
        "-y",
        "-loglevel",
        "error",
        ...inputs,
        "-filter_complex",
        filter,
        "-map",
        `[${last}]`,
        "-t",
        String(duration),
        "-c:v",
        "libx264",
        "-preset",
        "slow",
        "-crf",
        String(crf),
        "-pix_fmt",
        "yuv420p",
        "-an",
        "-movflags",
        "+faststart",
        mp4,
      ],
      { maxBuffer: 16 * 1024 * 1024 },
    );
    const { size: bytes } = await stat(mp4);
    if (bytes <= MAX_BYTES) {
      // The poster is the final frame: both devices showing the synced end state.
      await run("ffmpeg", [
        "-y",
        "-loglevel",
        "error",
        "-sseof",
        "-0.5",
        "-i",
        mp4,
        "-frames:v",
        "1",
        "-q:v",
        "3",
        poster,
      ]);
      return { mp4, poster, bytes, crf };
    }
  }
  throw new Error(`${id}: could not encode under ${MAX_BYTES} bytes`);
}
