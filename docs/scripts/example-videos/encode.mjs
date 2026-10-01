// Encodes a stage recording (./stage.mjs) into the committed, streaming-friendly
// H.264 MP4 (plays in every browser, including Safari) plus a JPEG poster.
import { execFile } from "node:child_process";
import { stat } from "node:fs/promises";
import { promisify } from "node:util";

const run = promisify(execFile);
/** Hard limit for a committed walkthrough video. */
export const MAX_BYTES = 5_000_000;
/** What we aim for: small enough to autoplay on a slow connection. */
export const TARGET_BYTES = 2_200_000;

/**
 * input: the stage's webm; trimStart: seconds to cut from the front;
 * posterAt: seconds (after trimming) for the poster frame, default the last
 * frame. Raises CRF until the clip fits TARGET_BYTES.
 */
export async function encodeRecording({ input, trimStart = 0, outDir, id, posterAt }) {
  const mp4 = `${outDir}${id}.mp4`;
  const poster = `${outDir}${id}.jpg`;
  let bytes = Infinity;
  let crf;
  for (crf of [26, 28, 30, 32, 34, 36]) {
    await run(
      "ffmpeg",
      [
        "-y",
        "-loglevel",
        "error",
        "-ss",
        trimStart.toFixed(3),
        "-i",
        input,
        "-vf",
        "fps=30",
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
    ({ size: bytes } = await stat(mp4));
    if (bytes <= TARGET_BYTES) break;
  }
  if (bytes > MAX_BYTES) throw new Error(`${id}: could not encode under ${MAX_BYTES} bytes`);
  const at = posterAt === undefined ? ["-sseof", "-0.5"] : ["-ss", String(posterAt)];
  await run("ffmpeg", [
    "-y",
    "-loglevel",
    "error",
    ...at,
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

/** Starts a dev server in its own process group; resolves once `ready` matches its output. */
export async function startServer(command, args, { cwd, env, ready, timeout = 180_000 }) {
  const { spawn } = await import("node:child_process");
  const child = spawn(command, args, {
    cwd,
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
  });
  let output = "";
  child.stdout.on("data", (chunk) => (output += chunk));
  child.stderr.on("data", (chunk) => (output += chunk));
  const stop = () => {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {}
  };
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Server did not start:\n${output}`)), timeout);
    const poll = setInterval(async () => {
      let ok = false;
      try {
        ok = typeof ready === "function" ? await ready(output) : ready.test(output);
      } catch {}
      if (ok) {
        clearInterval(poll);
        clearTimeout(timer);
        resolve();
      } else if (child.exitCode !== null) {
        clearInterval(poll);
        clearTimeout(timer);
        reject(new Error(`Server exited:\n${output}`));
      }
    }, 500);
  });
  return { stop, log: () => output };
}
