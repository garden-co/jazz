// Turns a Playwright WebM recording into a streaming-friendly H.264 MP4 (plays
// in every browser, including Safari) plus a JPEG poster, under a size budget.
import { execFile } from "node:child_process";
import { stat } from "node:fs/promises";
import { promisify } from "node:util";

const run = promisify(execFile);
export const MAX_BYTES = 5_000_000;

export async function encode(input, outDir, id) {
  const mp4 = `${outDir}${id}.mp4`;
  const poster = `${outDir}${id}.jpg`;
  // Screen recordings compress well; raise CRF until the clip fits the budget.
  for (const crf of [26, 30, 34, 38]) {
    await run("ffmpeg", [
      "-y",
      "-loglevel",
      "error",
      "-i",
      input,
      "-c:v",
      "libx264",
      "-preset",
      "slow",
      "-crf",
      String(crf),
      "-pix_fmt",
      "yuv420p",
      "-r",
      "30",
      "-an",
      "-movflags",
      "+faststart",
      mp4,
    ]);
    const { size } = await stat(mp4);
    if (size <= MAX_BYTES) {
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
      return { mp4, poster, bytes: size, crf };
    }
  }
  throw new Error(`${id}: could not encode under ${MAX_BYTES} bytes`);
}
