import { spawn, type ChildProcess } from "node:child_process";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { NodeCatalogueCache } from "./catalogue-cache-node.js";

const cacheEntry = new URL("../../dist/runtime/catalogue-cache-node.js", import.meta.url).href;
const nativeEntry = createRequire(import.meta.url).resolve("fs-native-extensions");
const scope = {
  registryAuthority: "https://registry.example/accounts",
  appId: "cache-process-test",
  environment: "test",
};
const children = new Set<ChildProcess>();
const roots: string[] = [];
function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}
afterEach(async () => {
  for (const child of children) {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = deferred();
      child.once("exit", () => exited.resolve());
      child.kill("SIGKILL");
      await exited.promise;
    }
  }
  children.clear();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

function childProgram(program: string) {
  const child = spawn(process.execPath, ["--input-type=module", "--eval", program], {
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  children.add(child);
  const ready = deferred();
  const exited = deferred();
  let errors = "";
  child.stderr!.on("data", (bytes: Buffer) => {
    errors += bytes.toString();
  });
  child.on("message", (message) => {
    if (message === "ready") ready.resolve();
  });
  child.once("error", (error) => {
    ready.reject(error);
    exited.reject(error);
  });
  child.once("exit", (code, signal) => {
    if (code === 0 || signal === "SIGKILL") exited.resolve();
    else exited.reject(new Error(`Catalogue child exited ${code}/${signal}: ${errors}`));
    ready.reject(new Error(`Catalogue child exited before readiness: ${errors}`));
  });
  // Some child programs exit without a readiness phase.
  void ready.promise.catch(() => {});
  void exited.promise.catch(() => {});
  return { child, ready: ready.promise, exited: exited.promise };
}

it("serializes competing process capture validation so the last valid lineage cannot regress", async () => {
  const directory = await mkdtemp(join(tmpdir(), "jazz-cache-race-"));
  roots.push(directory);
  const programs = [3, 1, 4, 2].map((generation) =>
    childProgram(`
    import { NodeCatalogueCache } from ${JSON.stringify(cacheEntry)};
    const cache = new NodeCatalogueCache(${JSON.stringify(directory)});
    try {
      await cache.publish(${JSON.stringify(scope)}, Uint8Array.of(${generation}), (previous, next) => {
        if (previous[0] > next[0]) throw new Error("regression");
      });
    } catch (error) { if (error.message !== "regression") throw error; }
  `),
  );
  await Promise.all(programs.map((program) => program.exited));
  expect(await new NodeCatalogueCache(directory).load(scope)).toEqual(Uint8Array.of(4));
}, 20_000);

it("a dead lock holder cannot strand future durable cache publication", async () => {
  const directory = await mkdtemp(join(tmpdir(), "jazz-cache-lock-death-"));
  roots.push(directory);
  const holder = childProgram(`
    import { createRequire } from "node:module";
    import { open } from "node:fs/promises";
    const { waitForLock } = createRequire(import.meta.url)(${JSON.stringify(nativeEntry)});
    const lock = await open(${JSON.stringify(join(directory, "catalogue.lock"))}, "a+", 0o600);
    await waitForLock(lock.fd);
    process.send("ready");
    process.on("message", () => { if (!lock) throw new Error("lost lock guard"); });
  `);
  await holder.ready;
  holder.child.kill("SIGKILL");
  await holder.exited;
  await new NodeCatalogueCache(directory).publish(scope, Uint8Array.of(9), (previous, next) => {
    if (previous[0]! > next[0]!) throw new Error("regression");
  });
  expect(await new NodeCatalogueCache(directory).load(scope)).toEqual(Uint8Array.of(9));
}, 20_000);
