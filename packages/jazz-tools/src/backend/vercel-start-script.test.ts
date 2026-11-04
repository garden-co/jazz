import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
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

  const trust = {
    issuer: "https://oidc.vercel.com/garden-co",
    audience: "https://vercel.com/garden-co",
    subject: "owner:garden-co:project:app:environment:production",
  };

  /** Run a start; resolves with the exit code, or "running" once it became the server. */
  async function start(
    version: string,
    tarballUrl = "http://127.0.0.1:9/none.tgz",
    workload = trust,
    blobs?: Parameters<typeof startScript>[3],
  ) {
    const script = join(root, `start-${Math.random()}.sh`);
    writeFileSync(script, startScript(version, tarballUrl, workload, blobs, root));
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

  it("starts a core whose large values go to S3 under the project's role", async () => {
    const bin = join(root, "data/bin/jazz-tools-1.0.0");
    writeFileSync(
      bin,
      `#!/bin/bash\ntrap 'exit 0' TERM\necho "$S3_BUCKET_NAME $AWS_REGION $AWS_ROLE_ARN $*" >> ${join(root, "servers.log")}\nwhile true; do sleep 0.05; done\n`,
    );
    chmodSync(bin, 0o755);
    const blobs = {
      bucket: "app-blobs",
      region: "us-east-1",
      roleArn: "arn:aws:iam::1:role/vercel",
      prefix: "it's/",
    };
    expect(await start("1.0.0", undefined, trust, blobs)).toBe("running");
    const line = read("servers.log")!;
    expect(line).toMatch(/^app-blobs us-east-1 arn:aws:iam::1:role\/vercel server /);
    expect(line).toContain(
      "--blobs s3 --s3-role-from-workload --blobs-environment production --s3-prefix it's/",
    );
  });

  it("starts a core whose large values go to the project's Blob store", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0", undefined, trust, { storeId: "abc", prefix: "it's/" })).toBe(
      "running",
    );
    expect(read("servers.log")).toContain(
      "--blobs vercel-blob --vercel-blob-token-from-workload --vercel-blob-store-id abc " +
        "--blobs-environment production --vercel-blob-prefix it's/ --trusted-workload-issuer",
    );
  });

  it("starts once, leaves the same version alone, and upgrades to a newer one", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0")).toBe("running");
    const first = read("tmp/jazz.pid");
    expect(read("data/version")).toBe("1.0.0");
    // The core generates its app id on the Drive, and runs as it.
    const appId = read("data/app-id");
    expect(appId).toMatch(/^[0-9a-f-]{36}$/);
    expect(read("servers.log")).toBe(
      `1.0.0 server ${appId} --port 1625 --data-dir ${root}/data/store --blobs sqlite ` +
        `--trusted-workload-issuer ${trust.issuer} --trusted-workload-audience ${trust.audience} ` +
        `--trusted-workload-subject ${trust.subject}`,
    );

    expect(await start("1.0.0")).toBe(0);
    expect(read("tmp/jazz.pid")).toBe(first);

    installFake("2.0.0");
    expect(await start("2.0.0")).toBe("running");
    expect(alive(first)).toBe(false);
    expect(read("tmp/jazz.version")).toBe("2.0.0");
    // The older binary is pruned: it never runs on this Drive again.
    expect(existsSync(join(root, "data/bin/jazz-tools-1.0.0"))).toBe(false);
  });

  it("never retargets a running core to another workload", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0")).toBe("running");
    const first = read("tmp/jazz.pid");
    expect(read("data/trust")).toBe(`${trust.issuer} ${trust.audience} ${trust.subject}`);
    const other = { ...trust, subject: "owner:garden-co:project:app:environment:preview" };
    expect(await start("1.0.0", undefined, other)).toBe(1);
    // With 2.0.0 already installed, nothing else can stop this start.
    installFake("2.0.0");
    expect(await start("2.0.0", undefined, other)).toBe(1);
    expect(alive(first)).toBe(true);
    expect(read("data/trust")).toContain(trust.subject);
    expect(read("servers.log")!.split("\n")).toHaveLength(1);
  });

  it("pins a core that an older script started, on its next start", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0")).toBe("running");
    rmSync(join(root, "data/trust"));
    expect(await start("1.0.0")).toBe(0);
    expect(read("data/trust")).toContain(trust.subject);
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
