// Installs the backend and the generator from release-npm, and proves the installed bytes are
// the tarballs'. Then `jazz-tools generate` writes every consumer's bindings, as an adopter's does.
import { spawn } from "node:child_process";
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";
import { ROOT, WORK, assert, fetchStep, filesUnder, run, sha256, timed } from "../lib/common.mjs";
import { isJazzName } from "./names.mjs";

const tarballs = join(WORK, "artifacts", "release-npm");
const dir = join(WORK, "npm");
const NAPI = ["darwin-arm64", "darwin-x64", "linux-arm64-gnu", "linux-x64-gnu", "win32-x64-msvc"];
const HERE_NAPI = `@garden-co/jazz-napi-${process.arch === "arm64" ? "linux-arm64-gnu" : "linux-x64-gnu"}`;

export async function installNpm() {
  const { version } = JSON.parse(readFileSync(join(WORK, "candidate.json"), "utf8"));
  rmSync(dir, { recursive: true, force: true });
  cpSync(join(ROOT, "backend"), dir, { recursive: true });
  cpSync(join(ROOT, "..", "adopter-gate", "t9", "oidc.ts"), join(dir, "oidc.ts"));

  const tgz = (name) => {
    const file = join(tarballs, `${name.replace(/^@/, "").replace("/", "-")}-${version}.tgz`);
    assert(existsSync(file), `release-npm has no ${file}`);
    return file;
  };
  const direct = ["jazz-tools", "jazz-wasm", "jazz-napi", HERE_NAPI];
  const names = ["jazz-tools", "jazz-wasm", "jazz-napi", ...NAPI.map((p) => `@garden-co/jazz-napi-${p}`)];
  const manifest = JSON.parse(readFileSync(join(dir, "package.json"), "utf8"));
  for (const name of direct) manifest.dependencies[name] = `file:${tgz(name)}`;
  manifest.overrides = Object.fromEntries(
    names.map((name) => [name, direct.includes(name) ? `$${name}` : `file:${tgz(name)}`]),
  );
  writeFileSync(join(dir, "package.json"), JSON.stringify(manifest, null, 2));

  const registry = spawn(process.execPath, [join(ROOT, "build", "registry.mjs")], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  try {
    const url = await new Promise((resolve, reject) => {
      createInterface({ input: registry.stdout }).once("line", (line) => resolve(line.split(" ")[1]));
      registry.once("exit", () => reject(new Error("the blocking registry didn't start")));
      registry.once("error", reject);
    });
    await timed("npm install", () =>
      fetchStep("npm", ["install", "--no-audit", "--no-fund", `--registry=${url}`], {
        cwd: dir,
        env: { ...process.env, npm_config_cache: join(WORK, "npm-cache") },
      }),
    );
  } finally {
    registry.kill();
  }

  const lock = JSON.parse(readFileSync(join(dir, "package-lock.json"), "utf8"));
  const checked = new Set();
  for (const [path, entry] of Object.entries(lock.packages)) {
    const name = path.split("node_modules/").at(-1);
    if (!isJazzName(name)) continue;
    assert(entry.resolved?.startsWith("file:"), `npm resolved ${name} from ${entry.resolved}`);
    if (!existsSync(join(dir, path))) continue; // another platform's napi
    const unpacked = join(WORK, "npm-expected", name);
    rmSync(unpacked, { recursive: true, force: true });
    mkdirSync(unpacked, { recursive: true });
    run("tar", ["-xzf", tgz(name), "-C", unpacked, "--strip-components=1"], { capture: true });
    const installed = realpathSync(join(dir, path));
    for (const file of filesUnder(unpacked)) {
      if (file === "package.json") continue; // npm may rewrite it
      assert(
        (await sha256(join(unpacked, file))) === (await sha256(join(installed, file))),
        `installed ${name}/${file} is not the candidate tarball's`,
      );
    }
    checked.add(name);
    console.log(`RESOLVED npm ${name} ${version} sha256 ${await sha256(tgz(name))}`);
  }
  for (const name of direct) assert(checked.has(name), `${name} wasn't installed`);
  generate("backend", {}); // the app the backend serves
}

/** Copies schema.ts beside `markers` under the npm dir and generates there. Returns that dir. */
export function generate(name, markers) {
  const out = join(dir, "generated", name);
  rmSync(out, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });
  cpSync(join(ROOT, "app", "schema.ts"), join(out, "schema.ts"));
  for (const [file, text] of Object.entries(markers)) {
    mkdirSync(dirname(join(out, file)), { recursive: true });
    writeFileSync(join(out, file), text);
  }
  run(join(dir, "node_modules", ".bin", "jazz-tools"), ["generate"], { cwd: out, capture: true });
  console.log(`GENERATED ${name}: ${readdirSync(out).join(" ")}`);
  return out;
}
