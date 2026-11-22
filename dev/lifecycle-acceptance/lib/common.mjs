// Shared helpers. Node built-ins only: this runs before anything is installed.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { fileURLToPath } from "node:url";

export const ROOT = resolve(fileURLToPath(new URL("..", import.meta.url)));
export const WORK = resolve(process.env.LA_WORK ?? join(tmpdir(), "jazz-lifecycle"));
export const LANGS = ["rust", "swift", "kotlin", "go"];
export const REPO = "garden-co/jazz";

/** A failed setup step. Nothing is tested after one. */
export function die(message) {
  console.log(`SETUP_FAIL ${message}`);
  process.exit(1);
}

export function assert(condition, message) {
  if (!condition) throw new Error(message);
}

export async function sha256(file) {
  const hash = createHash("sha256");
  await pipeline(createReadStream(file), hash);
  return hash.digest("hex");
}

/** Every regular file under `dir`, as paths relative to it. Symlinks are refused. */
export function filesUnder(dir, skip = () => false) {
  const out = [];
  const walk = (at) => {
    for (const entry of readdirSync(at, { withFileTypes: true })) {
      const path = join(at, entry.name);
      const name = relative(dir, path);
      if (skip(name)) continue;
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile()) out.push(name);
      else throw new Error(`${path} is neither a file nor a directory`);
    }
  };
  walk(dir);
  return out.sort();
}

export function run(command, args, { cwd, env, capture = false, timeout = 1_500_000 } = {}) {
  if (!capture) console.log(`==> ${command} ${args.join(" ")}`);
  const result = spawnSync(command, args, {
    cwd,
    env: env ?? process.env,
    timeout,
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
    stdio: capture ? ["ignore", "pipe", "pipe"] : "inherit",
  });
  if (result.error) throw new Error(`${command}: ${result.error.message}`);
  if (result.status !== 0)
    throw new Error(
      `${command} ${args[0] ?? ""} failed (${result.status ?? result.signal})` +
        (capture ? `: ${(result.stderr || result.stdout).slice(-4000)}` : ""),
    );
  return capture ? result.stdout : "";
}

const NETWORK =
  /status(?: code)?:? (?:429|5\d\d)|HTTP (?:429|5\d\d)|timed out|ETIMEDOUT|ECONNRESET|EAI_AGAIN|Could not resolve host|Connection reset|UnknownHost|TLS handshake timeout|download of .* failed|Could not (?:GET|HEAD)|unexpected EOF/i;

/** A network fetch, and only that, may be retried, and only on a network error. */
export function fetchStep(command, args, options = {}) {
  for (let attempt = 1; ; attempt++) {
    try {
      return run(command, args, { timeout: 300_000, ...options, capture: true });
    } catch (error) {
      if (attempt === 3 || !NETWORK.test(error.message)) throw error;
      console.log(`note: network error from ${command}, attempt ${attempt} of 3`);
      spawnSync("sleep", [String(attempt * 5)]);
    }
  }
}

export async function timed(label, body) {
  const started = Date.now();
  const value = await body();
  console.log(`TIME ${label} ${((Date.now() - started) / 1000).toFixed(1)}s`);
  return value;
}
