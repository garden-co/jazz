import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../../index.js";
import { createDb } from "../default-create-db.js";
import {
  acquireNodeForegroundNodeLease,
  nodeForegroundNodeLeaseDirectoryForTest,
  type NodeForegroundNodeLeaseOptions,
} from "./node-foreground-node-lease.js";
import { createBrowserAuthSessionKey } from "../browser-worker-config.js";

const roots: string[] = [];
let restoreCwd: (() => void) | undefined;
const publicRuntimeEntry = new URL("../../../dist/runtime/index.js", import.meta.url).href;
const memoryApp = s.defineApp({ notes: s.table({ title: s.string() }) });

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

  it("rejects a handoff high-water outside the native u64 HLC domain", async () => {
    const lease = await acquireNodeForegroundNodeLease(await isolatedOptions());
    await expect(lease.returnWithHighWater(1n << 64n)).rejects.toThrow(
      "Invalid Node foreground lease handoff",
    );
    // The failed handoff leaves its active claim quarantined; do not retire it
    // in this receipt because the tested invariant is exactly no reuse.
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

  it("keeps explicit memory mode filesystem-free after a real public write", async () => {
    const root = await mkdtemp(join(tmpdir(), "jazz-node-memory-foreground-lease-"));
    roots.push(root);
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(root);
    try {
      const db = await createDb({ appId: "memory-lease-test", driver: { type: "memory" } });
      db.insert(memoryApp.notes, { title: "first write" });
      await db.shutdown();
      await expect(readdir(join(root, ".jazz"))).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      cwdSpy.mockRestore();
    }
  });

  it("quarantines an unknown active claim rather than reclaiming it", async () => {
    const options = await isolatedOptions();
    const directory = nodeForegroundNodeLeaseDirectoryForTest(options);
    const deadNode = "11".repeat(16);
    await mkdir(join(directory, "active"), { recursive: true });
    await writeFile(
      join(directory, "active", deadNode),
      JSON.stringify({
        format: "jazz-node-foreground-node-leases-v1",
        node: deadNode,
        token: "00000000-0000-4000-8000-000000000000",
      }),
    );
    const lease = await acquireNodeForegroundNodeLease(options);
    expect(lease.node).not.toEqual(Buffer.from(deadNode, "hex"));
    await lease.retire();
  });

  it("fails closed on a malformed reusable receipt and retains its active claim", async () => {
    const options = await isolatedOptions();
    const directory = nodeForegroundNodeLeaseDirectoryForTest(options);
    const node = "22".repeat(16);
    await mkdir(join(directory, "reusable"), { recursive: true });
    await writeFile(join(directory, "reusable", node), "not-json");
    await expect(acquireNodeForegroundNodeLease(options)).rejects.toThrow(
      "Invalid Node foreground lease receipt",
    );
    expect(await readdir(join(directory, "active"))).toContain(node);
  });

  it("keeps app, environment, and auth namespaces isolated inside one cwd", async () => {
    const first = await isolatedOptions();
    const variants = [
      { ...first, appId: "another-app" },
      { ...first, env: "production" },
      { ...first, authScope: "another-user" },
    ];
    for (const variant of variants) {
      expect(nodeForegroundNodeLeaseDirectoryForTest(variant)).not.toBe(
        nodeForegroundNodeLeaseDirectoryForTest(first),
      );
    }
    const [firstLease, ...otherLeases] = await Promise.all([
      acquireNodeForegroundNodeLease(first),
      ...variants.map(acquireNodeForegroundNodeLease),
    ]);
    try {
      for (const lease of otherLeases) expect(firstLease.node).not.toEqual(lease.node);
    } finally {
      await Promise.all([firstLease.retire(), ...otherLeases.map((lease) => lease.retire())]);
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
      expect(await readdir(join(directory, "active"))).toHaveLength(2);

      await Promise.all([killChild(first.child), killChild(second.child)]);

      // A crash leaves both active claims quarantined. The recovered process
      // receives a fresh UUID, then returns only that UUID cleanly.
      const recovered = spawnPublicPersistentDb(root, appId, "close");
      await recovered.ready;
      await waitForExit(recovered.child);

      expect(await readdir(join(directory, "active"))).toHaveLength(2);
      const clean = await readdir(join(directory, "reusable"));
      expect(clean).toHaveLength(1);
      const cleanReplacementNode = clean[0];

      const reopened = spawnPublicPersistentDb(root, appId, "hold");
      await reopened.ready;
      try {
        const active = await readdir(join(directory, "active"));
        expect(active).toHaveLength(3);
        expect(active).toContain(cleanReplacementNode);
      } finally {
        await killChild(reopened.child);
      }
    } finally {
      await Promise.all([killChild(first.child), killChild(second.child)]);
    }
  }, 30_000);
});
