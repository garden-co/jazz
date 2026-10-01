// Runs one walkthrough: starts the example's dev server, records the story on
// a fresh stage (./stage.mjs) and writes docs/public/examples/videos/<id>.mp4
// and .jpg. Every walkthrough script calls `record` once.
//
// Needs the prebuilt jazz-tools, WASM and NAPI artifacts (`pnpm build:core`)
// and ffmpeg. CHROMIUM_PATH picks a Chromium build; DEBUG_DIR keeps
// screenshots of every device when a run fails.
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { encodeRecording, startServer } from "./encode.mjs";
import { Stage } from "./stage.mjs";

const repo = fileURLToPath(new URL("../../../", import.meta.url));
const outDir = fileURLToPath(new URL("../../public/examples/videos/", import.meta.url));

/**
 * id: output name. app: the example's directory, relative to the repo.
 * server({ dir }): { command, args, env, ready, before, after }, or several.
 * run(stage): the story. Call `stage.poster()` at the moment for the poster.
 */
export async function record({
  id,
  app,
  server,
  run,
  width = 1280,
  height = 800,
  captionSize = 22,
}) {
  const dir = join(repo, app);
  const specs = [server({ dir })].flat();
  const servers = [];
  const videoDir = await mkdtemp(join(tmpdir(), `walkthrough-${id}-`));
  let stage;
  let failed = false;
  try {
    for (const spec of specs) {
      await spec.before?.();
      servers.push(
        await startServer(spec.command, spec.args, {
          cwd: spec.cwd ?? dir,
          env: spec.env,
          ready: spec.ready,
          timeout: spec.timeout,
        }),
      );
      await spec.warm?.();
    }
    stage = await Stage.launch({ width, height, captionSize, videoDir });
    await run(stage);
    const { path, trimStart, posterAt } = await stage.finish();
    await mkdir(outDir, { recursive: true });
    const encoded = await encodeRecording({ input: path, trimStart, outDir, id, posterAt });
    console.log(
      `Wrote ${encoded.mp4} (${(encoded.bytes / 1e6).toFixed(2)} MB, crf ${encoded.crf}) and ${encoded.poster}`,
    );
  } catch (error) {
    failed = true;
    console.error(error);
    for (const s of servers) console.error(s.log().slice(-6000));
    await stage?.abort(process.env.DEBUG_DIR);
  } finally {
    for (const s of servers) await s.stop();
    for (const spec of specs) await spec.after?.();
    await stage?.cleanup();
  }
  if (failed) process.exit(1);
}

/** Server spec for a Vite example (the Jazz Vite plugin starts the sync server). */
export function viteServer({ dir, port, env, fresh = true }) {
  return {
    before: () =>
      fresh &&
      rm(join(dir, "node_modules/.cache/jazz-dev-server"), { recursive: true, force: true }),
    command: "pnpm",
    args: ["exec", "vite", "--port", String(port), "--strictPort", "--host", "127.0.0.1"],
    env,
    ready: /Local:\s+http/,
  };
}

/**
 * Server spec for a Next.js example (`withJazz` starts the sync server).
 * Starts from an empty sync server and build cache, and removes both after.
 */
export function nextServer({ dir, port, env }) {
  const origin = `http://127.0.0.1:${port}`;
  const clean = async () => {
    await rm(join(dir, ".next"), { recursive: true, force: true });
    await rm(join(dir, "node_modules/.cache/jazz-dev-server"), { recursive: true, force: true });
  };
  return {
    before: clean,
    command: "pnpm",
    args: ["exec", "next", "dev", "-H", "127.0.0.1", "-p", String(port)],
    env: { NEXT_PUBLIC_APP_ORIGIN: origin, ...env },
    ready: /Ready in/,
    timeout: 300_000,
    // The first request compiles the app; do it before the camera rolls.
    warm: async () => {
      const response = await fetch(origin, { signal: AbortSignal.timeout(600_000) });
      if (response.status >= 500) throw new Error(`${origin} answered ${response.status}`);
    },
    after: async () => {
      await clean();
      await promisify(execFile)("git", ["checkout", "--", "next-env.d.ts"], { cwd: dir }).catch(
        () => {},
      );
    },
  };
}
