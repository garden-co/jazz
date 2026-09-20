import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn, execFileSync } from "node:child_process";
import { once } from "node:events";
import {
  mkdtempSync,
  mkdirSync,
  writeFileSync,
  readFileSync,
  chmodSync,
  existsSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
const here = fileURLToPath(new URL(".", import.meta.url));
const sha = "a".repeat(40),
  other = "b".repeat(40),
  version = "2.0.0-alpha.56";
const hash = (file) => createHash("sha256").update(readFileSync(file)).digest("hex");
function workspace(t) {
  const root = mkdtempSync(join(tmpdir(), "jazz-acceptance-test-"));
  t.after(() => {
    const pidFile = join(root, "pid.json");
    if (existsSync(pidFile)) {
      const { parent } = JSON.parse(readFileSync(pidFile, "utf8"));
      try {
        process.kill(-parent, "SIGKILL");
      } catch (error) {
        if (error.code !== "ESRCH") throw error;
      }
    }
    rmSync(root, { recursive: true, force: true });
  });
  return root;
}
function run(t, script, config, env = {}) {
  const child = spawn(process.execPath, [join(here, script), config], {
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "",
    stderr = "";
  child.stdout.on("data", (b) => {
    stdout += b;
  });
  child.stderr.on("data", (b) => {
    stderr += b;
  });
  const exit = once(child, "exit").then(([code, signal]) => ({ code, signal, stdout, stderr }));
  t.after(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  });
  return { child, exit };
}
async function ready(file, child) {
  for (let i = 0; i < 200; i++) {
    if (existsSync(file)) return JSON.parse(readFileSync(file, "utf8"));
    assert(
      child.exitCode === null && child.signalCode === null,
      "Runner exited before synthetic child ready",
    );
    await delay(25);
  }
  throw new Error("Synthetic process readiness timeout");
}
function live(pid) {
  try {
    return !execFileSync("ps", ["-o", "stat=", "-p", String(pid)], { encoding: "utf8" })
      .trim()
      .startsWith("Z");
  } catch {
    return false;
  }
}
function packedFixture(t, nested) {
  const root = workspace(t),
    project = join(root, "project"),
    packages = {};
  mkdirSync(project);
  writeFileSync(join(project, "package.json"), "{}");
  const native = `@garden-co/jazz-napi-linux-${process.arch}-gnu`,
    fp = "fixture-fingerprint";
  for (const name of ["jazz-tools", "jazz-napi", "jazz-wasm", native]) {
    const directory = join(project, "node_modules", name);
    mkdirSync(directory, { recursive: true });
    writeFileSync(
      join(directory, "package.json"),
      JSON.stringify({ name, version, main: "index.cjs" }),
    );
    writeFileSync(
      join(directory, "index.cjs"),
      name === "jazz-napi"
        ? `exports.nativeArtifactFingerprint=()=>${JSON.stringify(fp)};`
        : "module.exports={};",
    );
    if (name === "jazz-tools") {
      mkdirSync(join(directory, "dist"));
      writeFileSync(join(directory, "dist", "consumer.cjs"), "require('jazz-napi');");
    }
    if (name === native)
      writeFileSync(
        join(directory, `jazz-napi.linux-${process.arch}-gnu.manifest.json`),
        JSON.stringify({
          git: { head: sha },
          profile: "release",
          nativeArtifactFingerprint: fp,
          artifacts: [],
        }),
      );
    const tarball = join(root, name.replaceAll("/", "_") + ".tgz");
    execFileSync("tar", ["-czf", tarball, "--transform=s,^,package/,", "-C", directory, "."]);
    packages[name] = { tarball, sha256: hash(tarball) };
  }
  if (nested) {
    const directory = join(project, "node_modules/jazz-tools", nested, "node_modules/jazz-napi");
    mkdirSync(directory, { recursive: true });
    writeFileSync(
      join(directory, "package.json"),
      JSON.stringify({ name: "jazz-napi", version, main: "index.cjs" }),
    );
    writeFileSync(join(directory, "index.cjs"), "throw new Error('unverified code executed');");
  }
  const cli = join(root, "cli"),
    pidFile = join(root, "pid.json");
  writeFileSync(
    cli,
    `#!${process.execPath}\nrequire('node:fs').writeFileSync(${JSON.stringify(pidFile)},JSON.stringify({parent:process.pid}));process.on('SIGTERM',()=>{});setInterval(()=>{},1000);`,
  );
  chmodSync(cli, 0o700);
  const producer = join(root, "producer.json");
  writeFileSync(
    producer,
    JSON.stringify({ git: { head: sha }, artifacts: [{ file: "cli", sha256: hash(cli) }] }),
  );
  const config = join(root, "config.json");
  writeFileSync(
    config,
    JSON.stringify({
      phase: "final-preview",
      sourceSha: sha,
      packageVersion: version,
      previewRun: "synthetic",
      project,
      cli,
      cliSha256: hash(cli),
      nativeFingerprint: fp,
      output: join(root, "receipt"),
      packages,
      cliProducer: { path: producer, sha256: hash(producer), artifact: "cli" },
    }),
  );
  return { root, config, pidFile };
}
for (const nested of [".", "dist"])
  test(
    `rejects unchecked Jazz dependency under ${nested} before CLI startup`,
    { skip: process.platform !== "linux", timeout: 10000 },
    async (t) => {
      const f = packedFixture(t, nested),
        attempt = run(t, "run.mjs", f.config);
      const r = await Promise.race([
        attempt.exit,
        ready(f.pidFile, attempt.child).then(() => {
          throw new Error("CLI started with unchecked dependency");
        }),
      ]);
      assert.equal(r.code, 1);
      assert.match(r.stderr, /Unverified Jazz dependency resolution/);
      assert(!existsSync(f.pidFile));
      assert(!r.stdout.includes('"check":"provenance"'));
    },
  );
for (const signal of ["SIGINT", "SIGTERM"])
  test(
    `runner ${signal} escalates and reaps owned CLI`,
    { skip: process.platform !== "linux", timeout: 10000 },
    async (t) => {
      const f = packedFixture(t),
        r = run(t, "run.mjs", f.config),
        { parent } = await ready(f.pidFile, r.child);
      r.child.kill(signal);
      const result = await r.exit;
      assert.equal(result.code, signal === "SIGINT" ? 130 : 143);
      assert(!live(parent), "CLI survived runner interruption");
    },
  );
function scaffoldFixture(t, { badInput = false, badDependency = false, longLived = false } = {}) {
  const root = workspace(t),
    bin = join(root, "bin"),
    pidFile = join(root, "pid.json");
  mkdirSync(bin);
  const dependency = `https://pkg.pr.new/garden-co/jazz/jazz-tools@${badDependency ? `${other}#${sha}` : sha}`;
  const program = longLived
    ? `const child=require('node:child_process').spawn(process.execPath,['-e',"process.on('SIGTERM',()=>{});setInterval(()=>{},1000)"],{stdio:'ignore'});require('node:fs').writeFileSync(${JSON.stringify(pidFile)},JSON.stringify({parent:process.pid,descendant:child.pid}));process.on('SIGTERM',()=>{});setInterval(()=>{},1000);`
    : `const fs=require('node:fs'),p=require('node:path');fs.mkdirSync(p.join(process.cwd(),'fixture'));fs.writeFileSync(p.join(process.cwd(),'fixture/package.json'),JSON.stringify({dependencies:{'jazz-tools':${JSON.stringify(dependency)}}}));`;
  writeFileSync(join(bin, "npm"), `#!${process.execPath}\n${program}`);
  chmodSync(join(bin, "npm"), 0o700);
  const config = join(root, "config.json");
  writeFileSync(
    config,
    JSON.stringify({
      phase: "final-preview",
      sourceSha: sha,
      packageVersion: version,
      parent: join(root, "parent"),
      name: "fixture",
      createJazzSpec: `https://pkg.pr.new/garden-co/jazz/create-jazz@${badInput ? `${other}#${sha}` : sha}`,
    }),
  );
  return { config, pidFile, env: { PATH: `${bin}:${process.env.PATH}` } };
}
for (const bad of ["badInput", "badDependency"])
  test(
    `scaffold rejects SHA-fragment spoof in ${bad}`,
    { skip: process.platform === "win32", timeout: 10000 },
    async (t) => {
      const f = scaffoldFixture(t, { [bad]: true }),
        r = await run(t, "scaffold.mjs", f.config, f.env).exit;
      assert.equal(r.code, 1);
      assert.match(r.stderr, /exact candidate locator|exact preview pin/);
      assert(!r.stdout.includes('"status":"PASS"'));
    },
  );
for (const signal of ["SIGINT", "SIGTERM"])
  test(
    `scaffold ${signal} cleans npm and its descendant`,
    { skip: process.platform === "win32", timeout: 10000 },
    async (t) => {
      const f = scaffoldFixture(t, { longLived: true }),
        r = run(t, "scaffold.mjs", f.config, f.env),
        pids = await ready(f.pidFile, r.child);
      r.child.kill(signal);
      const result = await r.exit;
      assert.equal(result.code, signal === "SIGINT" ? 130 : 143);
      assert(!live(pids.parent), "npm survived");
      assert(!live(pids.descendant), "npm descendant survived");
    },
  );
test(
  "scaffold accepts the exact candidate dependency locator",
  { skip: process.platform === "win32", timeout: 10000 },
  async (t) => {
    const f = scaffoldFixture(t),
      r = await run(t, "scaffold.mjs", f.config, f.env).exit;
    assert.equal(r.code, 0, r.stderr);
    assert(r.stdout.includes('"status":"PASS"'));
  },
);
