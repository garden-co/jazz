import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { startScript } from "./vercel-store.js";

// The start script as it runs in a Sandbox, against fake server binaries:
// bash scripts that log their version and wait to be stopped.
describe.skipIf(process.platform !== "linux")("vercel() start script", () => {
  let root: string;
  const children: ChildProcess[] = [];

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "jazz-start-"));
    mkdirSync(join(root, "data/bin"), { recursive: true });
    mkdirSync(join(root, "tmp"));
  });

  afterEach(() => {
    for (const child of children.splice(0)) child.kill("SIGKILL");
    const pid = read("tmp/jazz.pid");
    if (pid) spawnSync("kill", ["-9", pid]);
  });

  const read = (path: string) =>
    existsSync(join(root, path)) ? readFileSync(join(root, path), "utf8").trim() : undefined;

  function installFake(version: string) {
    const bin = join(root, `data/bin/jazz-tools-${version}`);
    writeFileSync(
      bin,
      `#!/bin/bash\ntrap 'exit 0' TERM\necho "${version} $*" >> ${join(root, "servers.log")}\nwhile true; do sleep 0.05; done\n`,
    );
    chmodSync(bin, 0o755);
  }

  /** Run a start; resolves with the exit code, or "running" once it became the server. */
  async function start(version: string, tarballUrl = "http://127.0.0.1:9/none.tgz") {
    const script = join(root, `start-${Math.random()}.sh`);
    writeFileSync(script, startScript("app-1", version, tarballUrl, root));
    const child = spawn("bash", [script], { stdio: "ignore", detached: true });
    children.push(child);
    const exited = new Promise<number>((resolve) =>
      child.on("exit", (code) => resolve(code ?? -1)),
    );
    const deadline = Date.now() + 10_000;
    for (;;) {
      const done = await Promise.race([exited, new Promise((r) => setTimeout(r, 50, "wait"))]);
      if (done !== "wait") return done as number;
      if (read("tmp/jazz.pid") === String(child.pid) && read("servers.log")) return "running";
      if (Date.now() > deadline) throw new Error("start neither exited nor ran a server");
    }
  }

  const alive = (pid: string | undefined) => !!pid && spawnSync("kill", ["-0", pid]).status === 0;

  it("starts once, leaves the same version alone, and upgrades to a newer one", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0")).toBe("running");
    const first = read("tmp/jazz.pid");
    expect(read("data/version")).toBe("1.0.0");
    expect(read("data/secrets.env")).toContain("JAZZ_APP_ID=app-1");

    expect(await start("1.0.0")).toBe(0);
    expect(read("tmp/jazz.pid")).toBe(first);

    installFake("2.0.0");
    expect(await start("2.0.0")).toBe("running");
    expect(alive(first)).toBe(false);
    expect(read("tmp/jazz.version")).toBe("2.0.0");
    // The older binary is pruned: it never runs on this Drive again.
    expect(existsSync(join(root, "data/bin/jazz-tools-1.0.0"))).toBe(false);
  });

  it("never moves a newer server back", async () => {
    installFake("2.0.0");
    expect(await start("2.0.0")).toBe("running");
    const running = read("tmp/jazz.pid");
    expect(await start("1.0.0")).toBe(0);
    expect(read("tmp/jazz.pid")).toBe(running);

    // With nothing running, an older start runs the newer server.
    spawnSync("kill", [running!]);
    await new Promise((r) => setTimeout(r, 200));
    expect(await start("1.0.0")).toBe("running");
    expect(read("tmp/jazz.version")).toBe("2.0.0");
  });

  it("refuses to start when the newer server this Drive needs is missing", async () => {
    installFake("1.0.0");
    writeFileSync(join(root, "data/version"), "9.0.0\n");
    expect(await start("1.0.0")).toBe(1);
    expect(read("data/version")).toBe("9.0.0");
    expect(existsSync(join(root, "data/bin/jazz-tools-1.0.0"))).toBe(true);
  });

  it("keeps the old server up when the new one fails to download", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0")).toBe("running");
    const running = read("tmp/jazz.pid");
    expect(await start("2.0.0")).not.toBe("running");
    expect(alive(running)).toBe(true);
    expect(read("data/version")).toBe("1.0.0");
  });

  it("ignores a stale pid file that names another process", async () => {
    installFake("1.0.0");
    const bystander = spawn("sleep", ["30"], { stdio: "ignore" });
    children.push(bystander);
    writeFileSync(join(root, "tmp/jazz.pid"), `${bystander.pid}\n`);
    writeFileSync(join(root, "tmp/jazz.version"), "0.9.0\n");
    expect(await start("1.0.0")).toBe("running");
    expect(alive(String(bystander.pid))).toBe(true);
  });

  it("runs one server when several instances start at once", async () => {
    installFake("1.0.0");
    const results = await Promise.all([start("1.0.0"), start("1.0.0"), start("1.0.0")]);
    expect(results.filter((result) => result === "running")).toHaveLength(1);
    expect(results.filter((result) => result === 0)).toHaveLength(2);
    expect(read("servers.log")!.split("\n")).toHaveLength(1);
  });
});
