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

// The start script as it runs in a Sandbox, against a fake `node` on PATH:
// it fails every download and, given a core addon, logs the core's version,
// app id and options, then waits to be stopped.
describe.skipIf(process.platform !== "linux")("vercel() start script", () => {
  let root: string;
  const children: ChildProcess[] = [];

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "jazz-start-"));
    mkdirSync(join(root, "data/bin"), { recursive: true });
    mkdirSync(join(root, "tmp"));
    mkdirSync(join(root, "fakebin"));
    writeFileSync(
      join(root, "fakebin/node"),
      [
        "#!/bin/bash",
        'if [[ "$2" == *fetch* ]]; then exit 1; fi',
        "trap 'exit 0' TERM",
        'version=$(basename "$3" .node); version=${version#jazz-napi-}',
        `echo "$version $4 \${S3_BUCKET_NAME:-} $5" >> ${join(root, "servers.log")}`,
        "while true; do sleep 0.05; done",
        "",
      ].join("\n"),
    );
    chmodSync(join(root, "fakebin/node"), 0o755);
  });

  afterEach(() => {
    for (const child of children.splice(0)) child.kill("SIGKILL");
    const pid = read("tmp/jazz.pid");
    if (pid) spawnSync("kill", ["-9", pid]);
  });

  const read = (path: string) =>
    existsSync(join(root, path)) ? readFileSync(join(root, path), "utf8").trim() : undefined;

  function installFake(version: string) {
    writeFileSync(join(root, `data/bin/jazz-napi-${version}.node`), "addon");
  }

  /** What the fake core logged, its options parsed. */
  const servers = () =>
    read("servers.log")!
      .split("\n")
      .map((line) => {
        const [version, appId, bucket, ...options] = line.split(" ");
        return { version, appId, bucket, options: JSON.parse(options.join(" ")) };
      });

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
    const child = spawn("bash", [script], {
      stdio: "ignore",
      detached: true,
      env: { ...process.env, PATH: `${join(root, "fakebin")}:${process.env.PATH}` },
    });
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
    installFake("1.0.0");
    const blobs = {
      bucket: "app-blobs",
      region: "us-east-1",
      roleArn: "arn:aws:iam::1:role/vercel",
      prefix: "it's/",
    };
    expect(await start("1.0.0", undefined, trust, blobs)).toBe("running");
    const [server] = servers();
    expect(server!.bucket).toBe("app-blobs");
    expect(server!.options.blobs).toEqual({
      kind: "s3",
      roleFromWorkload: true,
      environment: "production",
      prefix: "it's/",
    });
  });

  it("starts a core whose large values go to the project's Blob store", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0", undefined, trust, { storeId: "abc", prefix: "it's/" })).toBe(
      "running",
    );
    expect(servers()[0]!.options.blobs).toEqual({
      kind: "vercel-blob",
      tokenFromWorkload: true,
      storeId: "abc",
      environment: "production",
      prefix: "it's/",
    });
  });

  it("starts once, leaves the same version alone, and upgrades to a newer one", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0")).toBe("running");
    const first = read("tmp/jazz.pid");
    expect(read("data/version")).toBe("1.0.0");
    // The core generates its app id on the Drive, and runs as it, trusting
    // only the workload.
    const appId = read("data/app-id");
    expect(appId).toMatch(/^[0-9a-f-]{36}$/);
    expect(servers()).toEqual([
      {
        version: "1.0.0",
        appId,
        bucket: "",
        options: {
          port: 1625,
          dataDir: `${root}/data/store`,
          blobs: { kind: "sqlite" },
          trustedWorkload: trust,
        },
      },
    ]);

    expect(await start("1.0.0")).toBe(0);
    expect(read("tmp/jazz.pid")).toBe(first);

    installFake("2.0.0");
    expect(await start("2.0.0")).toBe("running");
    expect(alive(first)).toBe(false);
    expect(read("tmp/jazz.version")).toBe("2.0.0");
    // The older core is pruned: it never runs on this Drive again.
    expect(existsSync(join(root, "data/bin/jazz-napi-1.0.0.node"))).toBe(false);
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
    expect(existsSync(join(root, "data/bin/jazz-napi-1.0.0.node"))).toBe(true);
  });

  it("keeps the old server up when the new one fails to download", async () => {
    installFake("1.0.0");
    expect(await start("1.0.0")).toBe("running");
    const running = read("tmp/jazz.pid");
    expect(await start("2.0.0")).not.toBe("running");
    expect(alive(running)).toBe(true);
    expect(read("data/version")).toBe("1.0.0");
  });

  it("says why a core can't be downloaded, for the function to report", async () => {
    expect(await start("2.0.0", "http://127.0.0.1:9/missing.tgz")).toBe(1);
    const error = read("tmp/jazz.error")!;
    expect(error).toContain(
      "could not download the core for 2.0.0 from http://127.0.0.1:9/missing.tgz",
    );
    expect(error).toContain("coreTarballUrl");
    expect(existsSync(join(root, "data/bin/jazz-napi-2.0.0.node.tmp"))).toBe(false);
    // A later start that succeeds clears it.
    installFake("2.0.0");
    expect(await start("2.0.0")).toBe("running");
    expect(read("tmp/jazz.error")).toBeUndefined();
  });

  it("says why it won't run an older core than the Drive's", async () => {
    installFake("1.0.0");
    writeFileSync(join(root, "data/version"), "9.0.0\n");
    expect(await start("1.0.0")).toBe(1);
    expect(read("tmp/jazz.error")).toMatch(/9\.0\.0 owns this Drive but its core is missing/);
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
