// Renders one walkthrough: starts the example's dev server, plays its
// storyboard (walkthroughs/<id>.storyboard.ts) on a fresh stage (./stage.mjs)
// with the named actions from walkthroughs/<id>.mjs, and writes
// docs/public/examples/videos/<id>.mp4 and .jpg. See README.md.
//
// Needs the prebuilt jazz-tools, WASM and NAPI artifacts (`pnpm build:core`)
// and ffmpeg. CHROMIUM_PATH picks a Chromium build; DEBUG_DIR keeps
// screenshots of every device when a run fails.
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { encodeRecording, startServer } from "./encode.mjs";
import { Stage, sleep } from "./stage.mjs";

const repo = fileURLToPath(new URL("../../../", import.meta.url));
const outDir = fileURLToPath(new URL("../../public/examples/videos/", import.meta.url));
const walkthroughs = new URL("./walkthroughs/", import.meta.url);

/** Every walkthrough id, from the storyboards in walkthroughs/. */
export async function walkthroughIds() {
  const files = await readdir(walkthroughs);
  return files
    .filter((f) => f.endsWith(".storyboard.ts"))
    .map((f) => f.slice(0, -".storyboard.ts".length))
    .sort();
}

/** Loads a walkthrough's storyboard and its module (app, server, actions). */
export async function loadWalkthrough(id) {
  const { default: storyboard } = await import(new URL(`${id}.storyboard.ts`, walkthroughs).href);
  const module = await import(new URL(`${id}.mjs`, walkthroughs).href);
  if (storyboard.id !== id) throw new Error(`${id}.storyboard.ts has id "${storyboard.id}"`);
  return { storyboard, ...module };
}

/** Renders one walkthrough by id. Throws if the recording fails. */
export async function render(id) {
  const { storyboard, app, server, actions = {}, deviceOptions = {} } = await loadWalkthrough(id);
  const missing = [storyboard.offCamera ?? [], storyboard.opening, storyboard.beats]
    .flat()
    .filter((beat) => "do" in beat && !actions[beat.do])
    .map((beat) => beat.do);
  if (missing.length) throw new Error(`${id}: no action named ${missing.join(", ")}`);

  const { width = 1280, height = 800, captionSize = 22, webgl = false } = storyboard.stage ?? {};
  const dir = join(repo, app);
  const specs = [server({ dir })].flat();
  const servers = [];
  const videoDir = await mkdtemp(join(tmpdir(), `walkthrough-${id}-`));
  let stage;
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
    stage = await Stage.launch({ width, height, captionSize, webgl, videoDir });
    const pages = {};
    for (const [deviceId, device] of Object.entries(storyboard.devices)) {
      pages[deviceId] = await stage.device(deviceId, { ...deviceOptions, ...device });
      pages[deviceId].setDefaultTimeout(90_000);
    }
    const play = playBeats({ id, stage, pages, actions, state: {} });
    await play(storyboard.offCamera ?? []);
    await stage.start();
    await play(storyboard.opening);
    stage.roll();
    await play(storyboard.beats);

    const { path, trimStart, posterAt } = await stage.finish();
    await mkdir(outDir, { recursive: true });
    const encoded = await encodeRecording({ input: path, trimStart, outDir, id, posterAt });
    console.log(
      `Wrote ${encoded.mp4} (${(encoded.bytes / 1e6).toFixed(2)} MB, crf ${encoded.crf}) and ${encoded.poster}`,
    );
  } catch (error) {
    for (const s of servers) console.error(s.log().slice(-6000));
    await stage?.abort(process.env.DEBUG_DIR);
    throw error;
  } finally {
    for (const s of servers) await s.stop();
    for (const spec of specs) await spec.after?.();
    await stage?.cleanup();
  }
}

/**
 * Plays beats in order. Actions get `{ stage, on, page, pages, state }` and the
 * beat's `args`: `on` is the beat's device id and `page` its page; `state` carries
 * values from one beat to a later one (an invite link, a shape's id).
 */
function playBeats({ id, stage, pages, actions, state }) {
  const page = (deviceId) => {
    if (!pages[deviceId]) throw new Error(`${id}: no device "${deviceId}"`);
    return pages[deviceId];
  };
  // "{name}" or "{name.0}" in a caption is a value an action kept in `state`.
  const fill = (text) =>
    text.replace(/\{([\w.]+)\}/g, (_, path) => {
      const value = path.split(".").reduce((v, key) => v?.[key], state);
      if (value === undefined) throw new Error(`${id}: nothing in state for {${path}}`);
      return value;
    });
  return async (beats) => {
    for (const beat of beats) {
      if (process.env.WALK_TRACE) console.log("beat", JSON.stringify(beat));
      if ("caption" in beat) await stage.caption(fill(beat.caption), beat.hold);
      else if ("title" in beat) await stage.title(beat.title, beat.text, beat.hold);
      else if ("do" in beat)
        await actions[beat.do](
          { stage, on: beat.on, page: beat.on && page(beat.on), pages, state },
          ...(beat.args ?? []),
        );
      else if ("wait" in beat) await sleep(beat.wait);
      else if ("wifi" in beat) await stage.wifi(beat.on, beat.wifi === "on");
      else if ("full" in beat) await stage.full(beat.full);
      else if ("split" in beat) await stage.split(...beat.split, { scale: beat.scale });
      else if ("show" in beat) await stage.show(beat.show);
      else if ("poster" in beat) stage.poster();
      else if ("see" in beat)
        await page(beat.on)
          .getByText(beat.see, { exact: beat.exact })
          .first()
          .waitFor({ timeout: beat.timeout ?? 30_000 });
      else if ("notSee" in beat) {
        if (await page(beat.on).getByText(beat.notSee).count())
          throw new Error(`${id}: "${beat.notSee}" is on ${beat.on}: ${beat.because}`);
      } else throw new Error(`${id}: unknown beat ${JSON.stringify(beat)}`);
    }
  };
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
