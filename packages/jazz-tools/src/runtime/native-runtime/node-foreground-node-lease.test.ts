import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  acquireNodeForegroundNodeLease,
  nodeForegroundNodeLeaseDirectoryForTest,
  type NodeForegroundNodeLeaseOptions,
} from "./node-foreground-node-lease.js";
import { createBrowserAuthSessionKey } from "../browser-worker-config.js";

const roots: string[] = [];
let restoreCwd: (() => void) | undefined;
const publicRuntimeEntry = new URL("../../../dist/runtime/index.js", import.meta.url).href;

afterEach(async () => {
  restoreCwd?.();
  restoreCwd = undefined;
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function isolatedOptions(): Promise<NodeForegroundNodeLeaseOptions> {
  const root = await mkdtemp(join(tmpdir(), "jazz-node-foreground-lease-"));
  roots.push(root);
  const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(root);
  restoreCwd = () => cwdSpy.mockRestore();
  return { appId: "node-lease-test", env: "test", authScope: "test-user" };
}

type PublicNodeChild = {
  child: ChildProcess;
  ready: Promise<void>;
};

/**
 * Uses the built public `createDb` entrypoint in a separate Node process. The
 * lease pool must protect this surface, rather than merely an internal helper.
 */
function spawnPublicPersistentDb(
  root: string,
  appId: string,
  mode: "hold" | "close",
): PublicNodeChild {
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "--eval",
      [
        `import { createDb } from ${JSON.stringify(publicRuntimeEntry)};`,
        `const db = await createDb({ appId: ${JSON.stringify(appId)} });`,
        'process.stdout.write("ready\\n");',
        mode === "close"
          ? 'await db.shutdown(); process.stdout.write("closed\\n");'
          : "setInterval(() => {}, 1_000);",
      ].join("\n"),
    ],
    { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
  );
  const ready = new Promise<void>((resolve, reject) => {
    let output = "";
    let errors = "";
    const timeout = setTimeout(() => {
      reject(new Error(`public Node Db child did not become ready: ${errors}`));
    }, 10_000);
    child.once("error", fail);
    child.once("exit", (code, signal) => {
      if (!output.includes("ready\n"))
        fail(new Error(`public Node Db child exited ${code}/${signal}: ${errors}`));
    });
    child.stdout?.on("data", (chunk: Buffer) => {
      output += chunk.toString();
      if (output.includes("ready\n")) succeed();
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      errors += chunk.toString();
    });
    function succeed() {
      clearTimeout(timeout);
      child.off("error", fail);
      resolve();
    }
    function fail(error: Error) {
      clearTimeout(timeout);
      child.off("error", fail);
      reject(error);
    }
  });
  return { child, ready };
}

async function waitForExit(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve, reject) => {
    child.once("exit", () => resolve());
    child.once("error", reject);
  });
}

async function killChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGKILL");
  await waitForExit(child);
}

describe("Node foreground node leases", () => {
  it("reuses only a clean node and never lowers its HLC floor", async () => {
    const options = await isolatedOptions();
    const first = await acquireNodeForegroundNodeLease(options);
    const node = first.node;
    await first.returnWithHighWater(123456n);

    const reopened = await acquireNodeForegroundNodeLease(options);
    expect(reopened.node).toEqual(node);
    expect(reopened.confirmedTxTime).toBe(123456n);
    // A wall-clock rollback cannot lower the continuation floor.
    await reopened.returnWithHighWater(1n);

    const continued = await acquireNodeForegroundNodeLease(options);
    expect(continued.node).toEqual(node);
    expect(continued.confirmedTxTime).toBe(123456n);
    await continued.retire();
  });

  it("allocates distinct nodes to concurrent foreground processes", async () => {
    const options = await isolatedOptions();
    const [first, second] = await Promise.all([
      acquireNodeForegroundNodeLease(options),
      acquireNodeForegroundNodeLease(options),
    ]);
    expect(first.node).not.toEqual(second.node);
    await Promise.all([first.retire(), second.retire()]);
  });

  it("fails closed on a torn state receipt", async () => {
    const options = await isolatedOptions();
    const directory = nodeForegroundNodeLeaseDirectoryForTest(options);
    await mkdir(directory, { recursive: true });
    await writeFile(join(directory, "state.json"), "{torn", { flag: "w" });
    await expect(acquireNodeForegroundNodeLease(options)).rejects.toThrow(
      "Invalid Node foreground lease state",
    );
  });

  it("retires a dirty node after its definitely-dead owner leaves a stale lock", async () => {
    const options = await isolatedOptions();
    const directory = nodeForegroundNodeLeaseDirectoryForTest(options);
    const deadNode = "11".repeat(16);
    await mkdir(directory, { recursive: true });
    await writeFile(
      join(directory, "state.json"),
      JSON.stringify({
        format: "jazz-node-foreground-node-leases-v1",
        clean: [],
        dirty: [{ node: deadNode, confirmedTxTime: "900" }],
        retired: [],
      }),
    );
    await writeFile(
      join(directory, `slot-${deadNode}.lock`),
      JSON.stringify({
        pid: 999_999_999,
        host: hostname(),
        processStartIdentity: null,
        nonce: "00000000-0000-4000-8000-000000000000",
      }),
    );

    const lease = await acquireNodeForegroundNodeLease(options);
    expect(lease.node).not.toEqual(Buffer.from(deadNode, "hex"));
    await lease.retire();
  });

  it("fails closed rather than stealing a foreign or malformed state lock", async () => {
    const options = await isolatedOptions();
    const directory = nodeForegroundNodeLeaseDirectoryForTest(options);
    await mkdir(directory, { recursive: true });
    await writeFile(join(directory, "state.lock"), "not a receipt", { flag: "w" });
    await expect(acquireNodeForegroundNodeLease(options)).rejects.toThrow(
      "invalid foreground lease lock receipt",
    );

    await writeFile(
      join(directory, "state.lock"),
      JSON.stringify({
        pid: process.pid,
        host: "another-host",
        processStartIdentity: null,
        nonce: "00000000-0000-4000-8000-000000000000",
      }),
      { flag: "w" },
    );
    await expect(acquireNodeForegroundNodeLease(options)).rejects.toThrow(
      "foreground lease lock belongs to a different host",
    );
  });

  it("treats a recycled-PID receipt as abandoned and permanently retires its node", async () => {
    if (process.platform !== "linux") return;
    const options = await isolatedOptions();
    const directory = nodeForegroundNodeLeaseDirectoryForTest(options);
    const deadNode = "22".repeat(16);
    await mkdir(directory, { recursive: true });
    await writeFile(
      join(directory, "state.json"),
      JSON.stringify({
        format: "jazz-node-foreground-node-leases-v1",
        clean: [],
        dirty: [{ node: deadNode, confirmedTxTime: "900" }],
        retired: [],
      }),
    );
    await writeFile(
      join(directory, `slot-${deadNode}.lock`),
      JSON.stringify({
        pid: process.pid,
        host: hostname(),
        // Deliberately differs from this live process's /proc start tick.
        processStartIdentity: "recycled-pid",
        nonce: "00000000-0000-4000-8000-000000000000",
      }),
    );

    const lease = await acquireNodeForegroundNodeLease(options);
    expect(lease.node).not.toEqual(Buffer.from(deadNode, "hex"));
    await lease.retire();
    const state = JSON.parse(await readFile(join(directory, "state.json"), "utf8"));
    expect(state.retired).toContain(deadNode);
  });

  it("keeps app and auth namespaces isolated inside one cwd", async () => {
    const first = await isolatedOptions();
    const second = { ...first, appId: "another-app" };
    expect(nodeForegroundNodeLeaseDirectoryForTest(second)).not.toBe(
      nodeForegroundNodeLeaseDirectoryForTest(first),
    );
    const [firstLease, secondLease] = await Promise.all([
      acquireNodeForegroundNodeLease(first),
      acquireNodeForegroundNodeLease(second),
    ]);
    try {
      expect(firstLease.node).not.toEqual(secondLease.node);
    } finally {
      await Promise.all([firstLease.retire(), secondLease.retire()]);
    }
  });

  it("protects public default persistent Db creation across processes and retires crash owners", async () => {
    if (process.platform === "win32") return;
    const root = await mkdtemp(join(tmpdir(), "jazz-node-public-foreground-lease-"));
    roots.push(root);
    const appId = "node-public-lease-test";
    const options: NodeForegroundNodeLeaseOptions = {
      appId,
      env: "dev",
      authScope: createBrowserAuthSessionKey({ appId }),
    };
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(root);
    const directory = nodeForegroundNodeLeaseDirectoryForTest(options);
    cwdSpy.mockRestore();

    const first = spawnPublicPersistentDb(root, appId, "hold");
    const second = spawnPublicPersistentDb(root, appId, "hold");
    try {
      await Promise.all([first.ready, second.ready]);
      const whileLive = JSON.parse(await readFile(join(directory, "state.json"), "utf8"));
      expect(whileLive.dirty).toHaveLength(2);
      expect(new Set(whileLive.dirty.map((slot: { node: string }) => slot.node)).size).toBe(2);

      await Promise.all([killChild(first.child), killChild(second.child)]);

      // A fresh public Db creation detects both definitely-dead processes,
      // retires their nodes, and exposes only a new identity. It then closes
      // cleanly, making that new node the sole reusable one.
      const recovered = spawnPublicPersistentDb(root, appId, "close");
      await recovered.ready;
      await waitForExit(recovered.child);

      const afterRecovery = JSON.parse(await readFile(join(directory, "state.json"), "utf8"));
      expect(afterRecovery.dirty).toEqual([]);
      expect(afterRecovery.clean).toHaveLength(1);
      expect(afterRecovery.retired).toHaveLength(2);
      const cleanReplacementNode = afterRecovery.clean[0].node;

      const reopened = spawnPublicPersistentDb(root, appId, "hold");
      await reopened.ready;
      try {
        const afterReopen = JSON.parse(await readFile(join(directory, "state.json"), "utf8"));
        expect(afterReopen.dirty).toHaveLength(1);
        expect(afterReopen.dirty[0].node).toBe(cleanReplacementNode);
        expect(afterReopen.retired).toHaveLength(2);
      } finally {
        await killChild(reopened.child);
      }
    } finally {
      await Promise.all([killChild(first.child), killChild(second.child)]);
    }
  }, 30_000);
});
