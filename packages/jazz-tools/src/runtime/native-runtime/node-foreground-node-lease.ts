/**
 * Node-only durable foreground TxId lease pool.
 *
 * This module is dynamically imported only from a Node runtime source. Keep
 * every `node:` import here so browser and React Native bundles never resolve
 * filesystem code.
 */
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { link, mkdir, open, readFile, rename, unlink, type FileHandle } from "node:fs/promises";
import { hostname } from "node:os";
import { dirname, join, resolve } from "node:path";
import type { ForegroundNodeLease } from "../runtime-source.js";

const FORMAT = "jazz-node-foreground-node-leases-v1";
const STATE_FILE = "state.json";
const STATE_LOCK_FILE = "state.lock";

type StoredSlot = { node: string; confirmedTxTime: string };
type StoredState = {
  format: typeof FORMAT;
  clean: StoredSlot[];
  dirty: StoredSlot[];
  retired: string[];
};

export type NodeForegroundNodeLeaseOptions = {
  appId: string;
  env: string;
  /** Stable non-secret auth namespace; never stores claims or credentials. */
  authScope: string;
};

/**
 * Acquire a node identity for exactly one live Node process.
 *
 * Slot locks stay held for the process lifetime. A later process first turns
 * every dirty slot whose previous PID has exited into a permanently retired
 * UUID, then allocates only a clean or fresh node. There is intentionally no
 * expiry or PID-time heuristic that could reuse an uncertain identity.
 */
export async function acquireNodeForegroundNodeLease(
  options: NodeForegroundNodeLeaseOptions,
): Promise<ForegroundNodeLease> {
  const directory = leaseDirectory(options);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  return await withStateLock(directory, async () => {
    const state = await readState(directory);
    await retireAbandonedDirtySlots(directory, state);

    const slot = state.clean.pop() ?? {
      node: randomBytes(16).toString("hex"),
      confirmedTxTime: "0",
    };
    const slotLock = await acquireSlotLock(directory, slot.node);
    try {
      state.dirty.push(slot);
      // The dirty record is the admission point: never expose an identity
      // until its active ownership survives a process crash.
      await writeStateAtomically(directory, state);
      return new NodeForegroundNodeLease(directory, slot, slotLock);
    } catch (error) {
      // No caller received this identity. Best-effort release avoids leaving a
      // live-process lock that could otherwise block the still-clean slot
      // forever; a cleanup failure remains fail-closed and is reported.
      await slotLock.close().catch(() => undefined);
      await unlink(slotLockPath(directory, slot.node)).catch(() => undefined);
      throw error;
    }
  });
}

/** @internal Test-only locator for white-box durability receipts. */
export function nodeForegroundNodeLeaseDirectoryForTest(
  options: NodeForegroundNodeLeaseOptions,
): string {
  return leaseDirectory(options);
}

class NodeForegroundNodeLease implements ForegroundNodeLease {
  readonly node: Uint8Array;
  readonly confirmedTxTime: bigint;
  private finished = false;

  constructor(
    private readonly directory: string,
    private readonly slot: StoredSlot,
    private readonly slotLock: FileHandle,
  ) {
    this.node = Buffer.from(slot.node, "hex");
    this.confirmedTxTime = BigInt(slot.confirmedTxTime);
  }

  async returnWithHighWater(highWater: bigint): Promise<void> {
    if (this.finished || highWater < 0n) throw new Error("Invalid Node foreground lease handoff");
    await this.finish(async (state) => {
      const slot = takeDirtySlot(state, this.slot.node);
      // A corrupted/old caller must not move the durable floor backwards.
      const confirmedTxTime =
        highWater > BigInt(slot.confirmedTxTime) ? highWater : BigInt(slot.confirmedTxTime);
      state.clean.push({ ...slot, confirmedTxTime: confirmedTxTime.toString() });
    });
  }

  async retire(): Promise<void> {
    if (this.finished) return;
    await this.finish(async (state) => {
      const slot = takeDirtySlot(state, this.slot.node);
      state.retired.push(slot.node);
    });
  }

  private async finish(update: (state: StoredState) => Promise<void>): Promise<void> {
    try {
      await withStateLock(this.directory, async () => {
        const state = await readState(this.directory);
        await update(state);
        await writeStateAtomically(this.directory, state);
      });
      // Release only after the durable state has been atomically renamed and
      // fsynced. A failure leaves both lock and dirty record in place.
      await this.slotLock.close();
      await unlink(slotLockPath(this.directory, this.slot.node));
      this.finished = true;
    } catch (error) {
      throw new Error(`Node foreground lease handoff failed: ${asError(error).message}`);
    }
  }
}

function leaseDirectory(options: NodeForegroundNodeLeaseOptions): string {
  const namespace = createHash("sha256")
    .update(JSON.stringify([options.appId, options.env, options.authScope]))
    .digest("hex");
  return resolve(process.cwd(), ".jazz", "foreground-node-leases", "v1", namespace);
}

async function withStateLock<T>(directory: string, run: () => Promise<T>): Promise<T> {
  const lockPath = join(directory, STATE_LOCK_FILE);
  const handle = await acquireExclusiveLock(lockPath);
  try {
    return await run();
  } finally {
    await handle.close();
    await unlink(lockPath).catch((error: unknown) => {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    });
  }
}

async function acquireSlotLock(directory: string, node: string) {
  return await acquireExclusiveLock(slotLockPath(directory, node));
}

async function acquireExclusiveLock(path: string) {
  for (;;) {
    const receipt = `${JSON.stringify({
      pid: process.pid,
      host: hostname(),
      processStartIdentity: await processStartIdentity(process.pid),
      nonce: randomUUID(),
    })}\n`;
    const staging = `${path}.${process.pid}.${randomUUID()}.acquiring`;
    try {
      const staged = await open(staging, "wx", 0o600);
      try {
        await staged.writeFile(receipt);
        await staged.sync();
      } finally {
        await staged.close();
      }
      await link(staging, path);
      await unlink(staging);
      return await open(path, "r");
    } catch (error) {
      await unlink(staging).catch((unlinkError: unknown) => {
        if ((unlinkError as NodeJS.ErrnoException).code !== "ENOENT") throw unlinkError;
      });
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      if (await lockOwnerIsAlive(path)) {
        await new Promise((resolveWait) => setTimeout(resolveWait, 10));
        continue;
      }
      // A dead owner cannot complete a critical section. Removing its stale
      // lock lets the new process retire its dirty slot under the state lock.
      await unlink(path).catch((unlinkError: unknown) => {
        if ((unlinkError as NodeJS.ErrnoException).code !== "ENOENT") throw unlinkError;
      });
    }
  }
}

async function lockOwnerIsAlive(path: string): Promise<boolean> {
  try {
    const owner = parseLockOwner(await readFile(path, "utf8"));
    if (owner.host !== hostname()) {
      throw new Error("foreground lease lock belongs to a different host");
    }
    process.kill(owner.pid, 0);
    if (owner.processStartIdentity !== null) {
      const current = await processStartIdentity(owner.pid);
      if (current && current !== owner.processStartIdentity) return false;
    }
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ESRCH" || code === "ENOENT") return false;
    // EPERM and malformed/partial/foreign receipts fail closed: never steal.
    if (code === "EPERM") return true;
    throw error;
  }
}

type LockOwner = {
  pid: number;
  host: string;
  processStartIdentity: string | null;
  nonce: string;
};

function parseLockOwner(value: string): LockOwner {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error("invalid foreground lease lock receipt");
  }
  if (!parsed || typeof parsed !== "object")
    throw new Error("invalid foreground lease lock receipt");
  const owner = parsed as Partial<LockOwner>;
  if (
    !Number.isSafeInteger(owner.pid) ||
    Number(owner.pid) <= 0 ||
    typeof owner.host !== "string" ||
    owner.host.length === 0 ||
    (owner.processStartIdentity !== null && typeof owner.processStartIdentity !== "string") ||
    typeof owner.nonce !== "string" ||
    !/^[0-9a-f-]{36}$/i.test(owner.nonce)
  ) {
    throw new Error("invalid foreground lease lock receipt");
  }
  return owner as LockOwner;
}

async function processStartIdentity(pid: number): Promise<string | null> {
  if (process.platform !== "linux") return null;
  try {
    // Linux field 22 is the monotonic process-start tick. It distinguishes
    // recycled PIDs without assuming that procfs exists on another platform.
    return (await readFile(`/proc/${pid}/stat`, "utf8")).trim().split(" ")[21] ?? null;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

async function readState(directory: string): Promise<StoredState> {
  try {
    const state = JSON.parse(await readFile(join(directory, STATE_FILE), "utf8"));
    assertState(state);
    return state;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return emptyState();
    throw new Error(`Invalid Node foreground lease state: ${asError(error).message}`);
  }
}

async function retireAbandonedDirtySlots(directory: string, state: StoredState): Promise<void> {
  const remaining: StoredSlot[] = [];
  for (const slot of state.dirty) {
    if (await lockOwnerIsAlive(slotLockPath(directory, slot.node))) {
      remaining.push(slot);
    } else {
      state.retired.push(slot.node);
      await unlink(slotLockPath(directory, slot.node)).catch((error: unknown) => {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      });
    }
  }
  state.dirty = remaining;
}

async function writeStateAtomically(directory: string, state: StoredState): Promise<void> {
  assertState(state);
  const destination = join(directory, STATE_FILE);
  const temporary = join(directory, `.${STATE_FILE}.${process.pid}.${randomUUID()}.tmp`);
  const encoded = `${JSON.stringify(state)}\n`;
  const file = await open(temporary, "wx", 0o600);
  try {
    await file.writeFile(encoded);
    await file.sync();
  } finally {
    await file.close();
  }
  await rename(temporary, destination);
  // Durably publish the rename before a node is exposed/reused. Directory
  // fsync is POSIX-specific; failure is fail-closed rather than guessed away.
  const parent = await open(dirname(destination), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}

function emptyState(): StoredState {
  return { format: FORMAT, clean: [], dirty: [], retired: [] };
}

function takeDirtySlot(state: StoredState, node: string): StoredSlot {
  const index = state.dirty.findIndex((slot) => slot.node === node);
  if (index < 0) throw new Error("Node foreground lease is no longer active");
  const [slot] = state.dirty.splice(index, 1);
  if (!slot) throw new Error("Node foreground lease is no longer active");
  return slot;
}

function assertState(value: unknown): asserts value is StoredState {
  if (!value || typeof value !== "object") throw new Error("state is not an object");
  const state = value as Partial<StoredState>;
  if (
    state.format !== FORMAT ||
    !Array.isArray(state.clean) ||
    !Array.isArray(state.dirty) ||
    !Array.isArray(state.retired)
  ) {
    throw new Error("state has an incompatible format");
  }
  const nodes = new Set<string>();
  for (const slot of [...state.clean, ...state.dirty]) {
    if (!slot || typeof slot !== "object") throw new Error("invalid slot");
    if (
      !/^[0-9a-f]{32}$/.test(slot.node) ||
      !/^(0|[1-9][0-9]*)$/.test(slot.confirmedTxTime) ||
      nodes.has(slot.node)
    ) {
      throw new Error("invalid slot");
    }
    nodes.add(slot.node);
  }
  for (const node of state.retired) {
    if (!/^[0-9a-f]{32}$/.test(node) || nodes.has(node)) throw new Error("invalid retired slot");
    nodes.add(node);
  }
}

function slotLockPath(directory: string, node: string): string {
  return join(directory, `slot-${node}.lock`);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
